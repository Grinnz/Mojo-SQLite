package Mojo::SQLite;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;
use DBD::SQLite;
use DBD::SQLite::Constants qw(:database_connection_configuration_options :dbd_sqlite_string_mode);
use File::Spec::Functions 'catfile';
use File::Temp;
use Mojo::SQLite::Database;
use Mojo::SQLite::Migrations;
use Scalar::Util qw(blessed weaken);
use SQL::Abstract::Pg;
use URI;
use URI::db;

our $VERSION = '3.009';

has abstract => sub { SQL::Abstract::Pg->new(name_sep => '.', quote_char => '"') };
has 'auto_migrate';
has database_class  => 'Mojo::SQLite::Database';
has dsn             => sub { _url_from_file(shift->_tempfile)->dbi_dsn };
has max_connections => 1;
has migrations      => sub { Mojo::SQLite::Migrations->new(sqlite => shift) };
has options => sub {
  {
    AutoCommit          => 1,
    AutoInactiveDestroy => 1,
    PrintError          => 0,
    RaiseError          => 1,
    sqlite_string_mode  => DBD_SQLITE_STRING_MODE_UNICODE_FALLBACK,
    wal_mode            => 1,
  };
};
has 'parent';

sub new { @_ > 1 ? shift->SUPER::new->from_string(@_) : shift->SUPER::new }

sub db { $_[0]->database_class->new(dbh => $_[0]->_prepare, sqlite => $_[0]) }

sub from_filename { shift->from_string(_url_from_file(shift, shift)) }

sub from_string {
  my ($self, $str) = @_;
  return $self unless $str;
  return $self->parent($str) if blessed $str and $str->isa('Mojo::SQLite');

  my $url = URI->new($str);

  # Options
  my %options = $url->query_form;
  $url->query(undef);
  # don't set default string_mode if sqlite_unicode legacy option is set
  delete $self->options->{sqlite_string_mode} if exists $options{sqlite_unicode};
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

  # Fork-safety
  delete @$self{qw(pid queue)} unless ($self->{pid} //= $$) eq $$;

  while (my $dbh = shift @{$self->{queue} || []}) { return $dbh if $dbh->ping }
  
  my $dbh = DBI->connect($self->dsn, undef, undef, $self->options)
    // croak "DBI connection to @{[$self->dsn]} failed: $DBI::errstr"; # RaiseError disabled
  $dbh->sqlite_db_config(SQLITE_DBCONFIG_DQS_DDL, 0);
  $dbh->sqlite_db_config(SQLITE_DBCONFIG_DQS_DML, 0);
  if ($self->options->{wal_mode} and !$self->options->{no_wal}) {
    $dbh->do('pragma journal_mode=WAL');
    $dbh->do('pragma synchronous=NORMAL');
  }

  # Cache the last insert rowid on inserts
  weaken(my $weakdbh = $dbh);
  $dbh->sqlite_update_hook(sub {
    $weakdbh->{private_mojo_last_insert_id} = $_[3] if $_[0] == DBD::SQLite::INSERT;
  });

  $self->emit(connection => $dbh);

  return $dbh;
}

sub _enqueue {
  my ($self, $dbh) = @_;

  if (my $parent = $self->parent) { return $parent->_enqueue($dbh) }

  my $queue = $self->{queue} ||= [];
  push @$queue, $dbh if $dbh->{Active};
  shift @$queue while @$queue > $self->max_connections;
}

sub _prepare {
  my $self = shift;

  # Automatic migrations
  ++$self->{migrated} and $self->migrations->migrate
    if !$self->{migrated} && $self->auto_migrate;

  my $parent = $self->parent;
  return $parent ? $parent->_prepare : $self->_dequeue;
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

  # Get a database handle from the cache for multiple queries
  my $db = $sql->db;

  # Use SQL::Abstract to generate simple CRUD queries for you
  $db->insert('names', {name => 'Isabel'});
  my $id = $db->select('names', ['id'], {name => 'Isabel'})->hash->{id};
  $db->update('names', {name => 'Bel'}, {id => $id});
  $db->delete('names', {name => 'Bel'});

  # Insert a few rows in a transaction with SQL and placeholders
  eval {
    my $tx = $db->begin;
    $db->query('insert into names (name) values (?)', 'Sara');
    $db->query('insert into names (name) values (?)', 'Stefan');
    $tx->commit;
  };
  say $@ if $@;

  # Insert another row with SQL::Abstract and return the generated id
  say $db->insert('names', {name => 'Daniel'})->last_insert_id;
  
  # JSON roundtrip
  say $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->hash->{foo}{bar};

  # Select one row at a time
  my $results = $db->query('select * from names');
  while (my $next = $results->hash) {
    say $next->{name};
  }

  # Select all rows with SQL::Abstract
  say $_->{name} for $db->select('names')->hashes->each;

=head1 DESCRIPTION

L<Mojo::SQLite> is a tiny wrapper around L<DBD::SQLite> that makes
L<SQLite|https://www.sqlite.org/> a lot of fun to use with the
L<Mojolicious|https://mojolico.us> real-time web framework. Use all
L<SQL features|http://sqlite.org/lang.html> SQLite has to offer, generate CRUD
queries from data structures, and manage your database schema with migrations.

=head1 BASICS

Database and statement handles are cached automatically, so they can be reused
transparently to increase performance. And you can handle connection timeouts
gracefully by holding on to them only for short amounts of time.

  use Mojolicious::Lite;
  use Mojo::SQLite;

  helper sqlite => sub { state $sql = Mojo::SQLite->new('sqlite:test.db') };

  get '/' => sub ($c) {
    my $db = $c->sqlite->db;
    $c->render(json => $db->query(q{select datetime('now','localtime') as now})->hash);
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

All I/O and queries are performed synchronously, and SQLite's default journal
mode only supports concurrent reads from multiple processes while the database
is not being written. The "Write-Ahead Log" journal mode allows multiple
processes to read and write concurrently to the same database file (but only
one can write at a time). WAL mode is enabled by the C<wal_mode> option,
currently enabled by default, and persists when opening that same database in
the future.

  # Performed concurrently (concurrent with writing only with WAL journaling mode)
  my $pid = fork || die $!;
  say $sql->db->query(q{select datetime('now','localtime') as time})->hash->{time};
  exit unless $pid;

The C<no_wal> option prevents WAL mode from being enabled in new databases but
doesn't affect databases where it has already been enabled. C<wal_mode> may not
be set by default in a future release. See L<http://sqlite.org/wal.html> and
L<DBD::SQLite/"journal_mode"> for more information.

The L<double-quoted string literal misfeature
|https://sqlite.org/quirks.html#double_quoted_string_literals_are_accepted> is
disabled for all connections since Mojo::SQLite 3.003; use single quotes for
string literals and double quotes for identifiers, as is normally recommended.

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

=head1 EXAMPLES

This distribution also contains a well-structured example
L<blog application|https://github.com/Grinnz/Mojo-SQLite/tree/master/examples/blog>
you can use for inspiration. This application shows how to apply the MVC design
pattern in practice.

=head1 EVENTS

L<Mojo::SQLite> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 connection

  $sql->on(connection => sub ($sql, $dbh) {
    $dbh->do('pragma journal_size_limit=1000000');
  });

Emitted when a new database connection has been established.

=head1 ATTRIBUTES

L<Mojo::SQLite> implements the following attributes.

=head2 abstract

  my $abstract = $sql->abstract;
  $sql         = $sql->abstract(SQL::Abstract->new);

L<SQL::Abstract> object used to generate CRUD queries for
L<Mojo::SQLite::Database>, defaults to a L<SQL::Abstract::Pg> object with
C<name_sep> set to C<.> and C<quote_char> set to C<">.

  # Generate WHERE clause and bind values
  my($stmt, @bind) = $sql->abstract->where({foo => 'bar', baz => 'yada'});

L<SQL::Abstract::Pg> provides additional features to the L<SQL::Abstract>
query methods in L<Mojo::SQLite::Database> such as C<-json> and
C<limit>/C<offset>. The C<for> feature is not applicable to SQLite queries.

  $sql->db->select(['some_table', ['other_table', foo_id => 'id']],
    ['foo', [bar => 'baz'], \q{datetime('now') as dt}],
    {foo => 'value'},
    {order_by => 'foo', limit => 10, offset => 5, group_by => ['foo'], having => {baz => 'value'}});

  # Upsert supported since SQLite 3.24.0
  $sql->db->insert('some_table', {name => $name, value => $value},
    {on_conflict => [name => {value => \'"excluded"."value"'}]});

=head2 auto_migrate

  my $bool = $sql->auto_migrate;
  $sql     = $sql->auto_migrate($bool);

Automatically migrate to the latest database schema with L</"migrations">, as
soon as L</"db"> has been called for the first time.

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
C<1>.

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

Options for database handles, defaults to setting C<sqlite_string_mode> to
C<DBD_SQLITE_STRING_MODE_UNICODE_FALLBACK>, setting C<AutoCommit>,
C<AutoInactiveDestroy> and C<RaiseError>, and deactivating C<PrintError>.
Note that C<AutoCommit> and C<RaiseError> are considered mandatory, so
deactivating them would be very dangerous. See
L<DBI/"ATTRIBUTES COMMON TO ALL HANDLES"> and
L<DBD::SQLite/"DRIVER PRIVATE ATTRIBUTES"> for more information on available
options.

=head2 parent

  my $parent = $sql->parent;
  $sql       = $sql->parent(Mojo::SQLite->new);

Another L<Mojo::SQLite> object to use for connection management, instead of
establishing and caching our own database connections.

=head1 METHODS

L<Mojo::SQLite> inherits all methods from L<Mojo::EventEmitter> and implements
the following new ones.

=head2 new

  my $sql = Mojo::SQLite->new;
  my $sql = Mojo::SQLite->new('file:test.db);
  my $sql = Mojo::SQLite->new('sqlite:test.db');
  my $sql = Mojo::SQLite->new(Mojo::SQLite->new);

Construct a new L<Mojo::SQLite> object and parse connection string with
L</"from_string"> if necessary.

  # Customize configuration further
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:dbname=test.db');
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:uri=file:test.db?mode=memory');

  # Pass filename directly
  my $sql = Mojo::SQLite->new->from_filename($filename);

=head2 db

  my $db = $sql->db;

Get a database object based on L</"database_class"> (which is usually
L<Mojo::SQLite::Database>) for a cached or newly established database
connection. The L<DBD::SQLite> database handle will be automatically cached
again when that object is destroyed, so you can handle problems like connection
timeouts gracefully by holding on to it only for short amounts of time.

  # Add up all the money
  say $sql->db->select('accounts')
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
  
  # Readonly connection without WAL mode
  $sql->from_filename($filename, { ReadOnly => 1, no_wal => 1 });
  
  # Strict unicode strings and WAL mode
  use DBD::SQLite::Constants ':dbd_sqlite_string_mode';
  $sql->from_filename($filename, { sqlite_string_mode => DBD_SQLITE_STRING_MODE_UNICODE_STRICT, wal_mode => 1 });

=head2 from_string

  $sql = $sql->from_string('test.db');
  $sql = $sql->from_string('file:test.db');
  $sql = $sql->from_string('file:///C:/foo/bar.db');
  $sql = $sql->from_string('sqlite:C:%5Cfoo%5Cbar.db');
  $sql = $sql->from_string(Mojo::SQLite->new);

Parse configuration from connection string or use another L<Mojo::SQLite>
object as L</"parent">. Connection strings are parsed as URLs, so you should
construct them using a module like L<Mojo::URL>, L<URI::file>, or L<URI::db>.
For portability on non-Unix-like systems, either construct the URL with the
C<sqlite> scheme, or use L<URI::file/"new"> to construct a URL with the C<file>
scheme. A URL with no scheme will be parsed as a C<file> URL, and C<file> URLs
are parsed according to the current operating system. If specified, the
hostname must be C<localhost>. If the URL has a query string, it will be parsed
and applied to L</"options">.

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

  # Readonly connection without WAL mode
  $sql->from_string('data.db?ReadOnly=1&no_wal=1');

  # String unicode strings and WAL mode
  use DBD::SQLite::Constants ':dbd_sqlite_string_mode';
  $sql->from_string(Mojo::URL->new->scheme('sqlite')->path('data.db')
    ->query(sqlite_string_mode => DBD_SQLITE_STRING_MODE_UNICODE_STRICT, wal_mode => 1));

=head1 DEBUGGING

You can set the C<DBI_TRACE> environment variable to get some advanced
diagnostics information printed by L<DBI>.

  DBI_TRACE=1
  DBI_TRACE=15
  DBI_TRACE=SQL

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
