package Mojo::SQLite;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;
use File::Spec::Functions 'catfile';
use File::Temp;
use Mojo::SQLite::Database;
use Mojo::SQLite::Migrations;
use Mojo::SQLite::PubSub;
use Scalar::Util 'weaken';
use URI;
use URI::db;

our $VERSION = '1.002';

has 'auto_migrate';
has database_class  => 'Mojo::SQLite::Database';
has dsn             => sub { _url_from_file(shift->_tempfile)->dbi_dsn };
has max_connections => 5;
has migrations      => sub {
  my $migrations = Mojo::SQLite::Migrations->new(sqlite => shift);
  weaken $migrations->{sqlite};
  return $migrations;
};
has options => sub {
  {
    AutoCommit          => 1,
    AutoInactiveDestroy => 1,
    PrintError          => 0,
    RaiseError          => 1,
    sqlite_unicode      => 1,
  };
};
has pubsub => sub {
  my $pubsub = Mojo::SQLite::PubSub->new(sqlite => shift);
  weaken $pubsub->{sqlite};
  return $pubsub;
};

sub new { @_ > 1 ? shift->SUPER::new->from_string(@_) : shift->SUPER::new }

sub db {
  my $self = shift;

  # Fork-safety
  delete @$self{qw(pid queue)} unless ($self->{pid} //= $$) eq $$;

  return $self->database_class->new(dbh => $self->_dequeue, sqlite => $self);
}

sub from_filename { shift->from_string(_url_from_file(shift, shift)) }

sub from_string {
  my ($self, $str) = @_;
  return $self unless $str;
  my $url = URI->new($str);

  # Options
  my %options = $url->query_form;
  $url->query(undef);
  @{$self->options}{keys %options} = values %options;

  # Parse URL based on scheme
  $url->scheme('file') unless $url->has_recognized_scheme;
  if ($url->scheme eq 'file') {
    $url = _url_from_file($url->file);
  } elsif ($url->scheme ne 'db') {
    $url = URI::db->new($url);
  }

  croak qq{Invalid SQLite connection string "$str"}
    unless $url->has_recognized_engine and $url->canonical_engine eq 'sqlite'
    and (($url->host // '') eq '' or $url->host eq 'localhost');
  
  # Temp database file
  $url->dbname($self->_tempfile) if $url->dbname eq ':temp:';
  
  return $self->dsn($url->dbi_dsn);
}

sub _dequeue {
  my $self = shift;
  while (my $dbh = shift @{$self->{queue} || []}) { return $dbh if $dbh->ping }
  my $dbh = DBI->connect($self->dsn, undef, undef, $self->options);
  if (defined $dbh) {
    $dbh->do('pragma journal_mode=WAL');
    $dbh->do('pragma synchronous=NORMAL');
  }
  ++$self->{migrated} and $self->migrations->migrate
    if !$self->{migrated} && $self->auto_migrate;
  $self->emit(connection => $dbh);
  return $dbh;
}

sub _enqueue {
  my ($self, $dbh) = @_;
  my $queue = $self->{queue} ||= [];
  push @$queue, $dbh if $dbh->{Active};
  shift @$queue while @$queue > $self->max_connections;
}

sub _tempfile { catfile(shift->{tempdir} = File::Temp->newdir, 'sqlite.db') }

sub _url_from_file {
  my $url = URI::db->new;
  $url->engine('sqlite');
  $url->dbname(shift);
  if (my $options = shift) { $url->query_form($options) }
  return $url;
}

1;

=head1 NAME

Mojo::SQLite - A tiny Mojolicious wrapper for SQLite

=head1 SYNOPSIS

  use Mojo::SQLite;

  # Select the library version
  my $sql = Mojo::SQLite->new('sqlite:test.db');
  say $sql->db->query('select sqlite_version() as version')->hash->{version};

  # Use migrations to create a table
  $sql->migrations->name('my_names_app')->from_string(<<EOF)->migrate;
  -- 1 up
  create table names (id integer primary key autoincrement, name text);
  -- 1 down
  drop table names;
  EOF

  # Use migrations to drop and recreate the table
  $sql->migrations->migrate(0)->migrate;

  # Insert a few rows
  my $db = $sql->db;
  $db->query('insert into names (name) values (?)', 'Sara');
  $db->query('insert into names (name) values (?)', 'Stefan');

  # Insert more rows in a transaction
  eval {
    my $tx = $db->begin;
    $db->query('insert into names (name) values (?)', 'Baerbel');
    $db->query('insert into names (name) values (?)', 'Wolfgang');
    $tx->commit;
  };
  say $@ if $@;

  # Insert another row and return the generated id
  say $db->query('insert into names (name) values (?)', 'Daniel')
    ->last_insert_id;
  
  # JSON roundtrip
  say $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->hash->{foo}{bar};

  # Select one row at a time
  my $results = $db->query('select * from names');
  while (my $next = $results->hash) {
    say $next->{name};
  }

  # Select all rows
  say $_->{name} for $db->query('select * from names')->hashes->each;

=head1 DESCRIPTION

L<Mojo::SQLite> is a tiny wrapper around L<DBD::SQLite> that makes
L<SQLite|https://www.sqlite.org/> a lot of fun to use with the
L<Mojolicious|https://mojolico.us> real-time web framework.

Database and statement handles are cached automatically, so they can be reused
transparently to increase performance. And you can handle connection timeouts
gracefully by holding on to them only for short amounts of time.

  use Mojolicious::Lite;
  use Mojo::SQLite;

  helper sqlite => sub { state $sql = Mojo::SQLite->new('sqlite:test.db') };

  get '/' => sub {
    my $c  = shift;
    my $db = $c->sqlite->db;
    $c->render(json => $db->query('select datetime("now","localtime") as now')->hash);
  };

  app->start;

In this example application, we create a C<sqlite> helper to store a
L<Mojo::SQLite> object. Our action calls that helper and uses the method
L<Mojo::SQLite/"db"> to dequeue a L<Mojo::SQLite::Database> object from the
connection pool. Then we use the method L<Mojo::SQLite::Database/"query"> to
execute an L<SQL|http://www.postgresql.org/docs/current/static/sql.html>
statement, which returns a L<Mojo::SQLite::Results> object. And finally we call
the method L<Mojo::SQLite::Results/"hash"> to retrieve the first row as a hash
reference.

All I/O and queries are performed synchronously. However, the "Write-Ahead Log"
journal is enabled for all connections, allowing multiple processes to read and
write concurrently to the same database file (but only one can write at a
time). See L<http://sqlite.org/wal.html> for more information.

  # Performed concurrently
  my $pid = fork || die $!;
  say $sql->db->query('select datetime("now","localtime") as time')->hash->{time};
  exit unless $pid;

All cached database handles will be reset automatically if a new process has
been forked, this allows multiple processes to share the same L<Mojo::SQLite>
object safely.

Any database errors will throw an exception as C<RaiseError> is automatically
enabled, so use C<eval> or L<Try::Tiny> to catch them. This makes transactions
with L<Mojo::SQLite::Database/"begin"> easy.

While passing a file path of C<:memory:> (or a custom L</"dsn"> with
C<mode=memory>) will create a temporary database, in-memory databases cannot be
shared between connections, so subsequent calls to L</"db"> may return
connections to completely different databases. For a temporary database that
can be shared between connections and processes, pass a file path of C<:temp:>
to store the database in a temporary directory (this is the default), or
consider constructing a temporary directory yourself with L<File::Temp> if you
need to reuse the filename. A temporary directory allows SQLite to create
L<additional temporary files|https://www.sqlite.org/tempfiles.html> safely.

  use File::Spec::Functions 'catfile';
  use File::Temp;
  use Mojo::SQLite;
  my $tempdir = File::Temp->newdir; # Deleted when object goes out of scope
  my $tempfile = catfile $tempdir, 'test.db';
  my $sql = Mojo::SQLite->new->from_filename($tempfile);

=head1 EVENTS

L<Mojo::SQLite> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 connection

  $sql->on(connection => sub {
    my ($sql, $dbh) = @_;
    $dbh->do('pragma journal_size_limit=1000000');
  });

Emitted when a new database connection has been established.

=head1 ATTRIBUTES

L<Mojo::SQLite> implements the following attributes.

=head2 auto_migrate

  my $bool = $sql->auto_migrate;
  $sql     = $sql->auto_migrate($bool);

Automatically migrate to the latest database schema with L</"migrations">, as
soon as the first database connection has been established.

=head2 database_class

  my $class = $sql->database_class;
  $sql      = $sql->database_class('MyApp::Database');

Class to be used by L</"db">, defaults to L<Mojo::SQLite::Database>. Note that
this class needs to have already been loaded before L</"db"> is called.

=head2 dsn

  my $dsn = $sql->dsn;
  $sql    = $sql->dsn('dbi:SQLite:uri=file:foo.db');

Data source name, defaults to C<dbi:SQLite:dbname=> followed by a path to a
temporary file.

=head2 max_connections

  my $max = $sql->max_connections;
  $sql    = $sql->max_connections(3);

Maximum number of idle database handles to cache for future use, defaults to
C<5>.

=head2 migrations

  my $migrations = $sql->migrations;
  $sql           = $sql->migrations(Mojo::SQLite::Migrations->new);

L<Mojo::SQLite::Migrations> object you can use to change your database schema
more easily.

  # Load migrations from file and migrate to latest version
  $sql->migrations->from_file('/home/dbook/migrations.sql')->migrate;

=head2 options

  my $options = $sql->options;
  $sql        = $sql->options({AutoCommit => 1, RaiseError => 1});

Options for database handles, defaults to activating C<sqlite_unicode>,
C<AutoCommit>, C<AutoInactiveDestroy> as well as C<RaiseError> and deactivating
C<PrintError>. Note that C<AutoCommit> and C<RaiseError> are considered
mandatory, so deactivating them would be very dangerous. See
L<DBI/"ATTRIBUTES COMMON TO ALL HANDLES"> and
L<DBD::SQLite/"DRIVER PRIVATE ATTRIBUTES"> for more information on available
options.

=head2 pubsub

This attribute is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

=head1 METHODS

L<Mojo::SQLite> inherits all methods from L<Mojo::EventEmitter> and implements
the following new ones.

=head2 new

  my $sql = Mojo::SQLite->new;
  my $sql = Mojo::SQLite->new('file:test.db);
  my $sql = Mojo::SQLite->new('sqlite:test.db');

Construct a new L<Mojo::SQLite> object and parse connection string with
L</"from_string"> if necessary.

  # Customize configuration further
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:dbname=test.db');
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:uri=file:test.db?mode=memory');

  # Pass filename directly
  my $sql = Mojo::SQLite->new->from_filename($filename);

=head2 db

  my $db = $sql->db;

Get a database object based on L</"database_class"> for a cached or newly
established database connection. The L<DBD::SQLite> database handle will be
automatically cached again when that object is destroyed, so you can handle
problems like connection timeouts gracefully by holding on to it only for short
amounts of time.

  # Add up all the money
  say $sql->db->query('select * from accounts')
    ->hashes->reduce(sub { $a->{money} + $b->{money} });

=head2 from_filename

  $sql = $sql->from_filename('C:\\Documents and Settings\\foo & bar.db', $options);

Parse database filename directly. Unlike L</"from_string">, the filename is
parsed as a local filename and not a URL. A hashref of L</"options"> may be
passed as the second argument.

  # Absolute filename
  $sql->from_filename('/home/fred/data.db');

  # Relative to current directory
  $sql->from_filename('data.db');

  # Temporary file database (default)
  $sql->from_filename(':temp:');

  # In-memory temporary database (single connection only)
  my $db = $sql->from_filename(':memory:')->db;

  # Additional options
  $sql->from_filename($filename, { PrintError => 1 });

=head2 from_string

  $sql = $sql->from_string('test.db');
  $sql = $sql->from_string('file:test.db');
  $sql = $sql->from_string('file:///C:/foo/bar.db');
  $sql = $sql->from_string('sqlite:C:%5Cfoo%5Cbar.db');

Parse configuration from connection string. Connection strings are parsed as
URLs, so you should construct them using a module like L<Mojo::URL>,
L<URI::file>, or L<URI::db>. For portability on non-Unix-like systems, either
construct the URL with the C<sqlite> scheme, or use L<URI::file/"new"> to
construct a URL with the C<file> scheme. A URL with no scheme will be parsed as
a C<file> URL, and C<file> URLs are parsed according to the current operating
system. If specified, the hostname must be C<localhost>. If the URL has a query
string, it will be parsed and applied to L</"options">.

  # Absolute filename
  $sql->from_string('sqlite:////home/fred/data.db');
  $sql->from_string('sqlite://localhost//home/fred/data.db');
  $sql->from_string('sqlite:/home/fred/data.db');
  $sql->from_string('file:///home/fred/data.db');
  $sql->from_string('file://localhost/home/fred/data.db');
  $sql->from_string('file:/home/fred/data.db');
  $sql->from_string('///home/fred/data.db');
  $sql->from_string('//localhost/home/fred/data.db');
  $sql->from_string('/home/fred/data.db');

  # Relative to current directory
  $sql->from_string('sqlite:data.db');
  $sql->from_string('file:data.db');
  $sql->from_string('data.db');

  # Connection string must be a valid URL
  $sql->from_string(Mojo::URL->new->scheme('sqlite')->path($filename));
  $sql->from_string(URI::db->new->Mojo::Base::tap(engine => 'sqlite')->Mojo::Base::tap(dbname => $filename));
  $sql->from_string(URI::file->new($filename));

  # Temporary file database (default)
  $sql->from_string(':temp:');

  # In-memory temporary database (single connection only)
  my $db = $sql->from_string(':memory:')->db;

  # Additional options
  $sql->from_string('data.db?PrintError=1&sqlite_allow_multiple_statements=1');
  $sql->from_string(Mojo::URL->new->scheme('sqlite')->path($filename)->query(sqlite_see_if_its_a_number => 1));
  $sql->from_string(URI::file->new($filename)->Mojo::Base::tap(query_form => {PrintError => 1}));

=head1 REFERENCE

This is the class hierarchy of the L<Mojo::SQLite> distribution.

=over 2

=item * L<Mojo::SQLite>

=item * L<Mojo::SQLite::Database>

=item * L<Mojo::SQLite::Migrations>

=item * L<Mojo::SQLite::Results>

=item * L<Mojo::SQLite::Transaction>

=back

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book, C<dbook@cpan.org>

=head1 CREDITS

Sebastian Riedel, author of L<Mojo::Pg>, which this distribution is based on.

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious>, L<Mojo::Pg>, L<DBD::SQLite>
