package Mojo::SQLite;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;
use File::Temp;
use Mojo::SQLite::Database;
use Mojo::SQLite::Migrations;
use Mojo::URL;
use Scalar::Util 'weaken';

our $VERSION = '0.008';

has dsn => sub {
  my $uri = Mojo::URL->new->scheme('file')->path(shift->_tempfile);
  return "dbi:SQLite:uri=$uri";
};
has max_connections => 5;
has migrations      => sub {
  my $migrations = Mojo::SQLite::Migrations->new(sqlite => shift);
  weaken $migrations->{sqlite};
  return $migrations;
};
has [qw(password username)];
has options => sub {
  {
    AutoCommit          => 1,
    AutoInactiveDestroy => 1,
    PrintError          => 0,
    RaiseError          => 1,
    sqlite_unicode      => 1,
  };
};

sub db {
  my $self = shift;

  # Fork-safety
  delete @$self{qw(pid queue)} unless ($self->{pid} //= $$) eq $$;

  return Mojo::SQLite::Database->new(dbh => $self->_dequeue, sqlite => $self);
}

sub from_string {
  my ($self, $str) = @_;

  # Protocol
  return $self unless $str;
  my $url = Mojo::URL->new($str);
  my $protocol = $url->protocol;
  my $host = $url->host // '';
  my $path = $url->path;
  my $options = $url->query->to_hash;
  croak qq{Invalid SQLite connection string "$str"}
    unless ($protocol eq '' or $protocol eq 'file')
    and ($host eq '' or $host eq 'localhost');

  # Database file
  $path = $self->_tempfile if $path eq ':temp:';
  my $uri = Mojo::URL->new->scheme('file')->path($path);
  my $dsn = "dbi:SQLite:uri=$uri";

  # Options
  @{$self->options}{keys %$options} = values %$options;

  return $self->dsn($dsn);
}

sub new { @_ > 1 ? shift->SUPER::new->from_string(@_) : shift->SUPER::new }

sub _dequeue {
  my $self = shift;
  while (my $dbh = shift @{$self->{queue} || []}) { return $dbh if $dbh->ping }
  my $dbh = DBI->connect(map { $self->$_ } qw(dsn username password options));
  $dbh->do('pragma journal_mode=WAL');
  $dbh->do('pragma synchronous=NORMAL');
  $self->emit(connection => $dbh);
  return $dbh;
}

sub _enqueue {
  my ($self, $dbh) = @_;
  my $queue = $self->{queue} ||= [];
  push @$queue, $dbh if $dbh->{Active};
  shift @$queue while @$queue > $self->max_connections;
}

sub _tempfile {
  my $self = shift;
  $self->{tempfile} = File::Temp->new(EXLOCK => 0);
  return $self->{tempfile}->filename;
};

1;

=head1 NAME

Mojo::SQLite - A tiny Mojolicious wrapper for SQLite

=head1 SYNOPSIS

  use Mojo::SQLite;
  use Mojo::URL;

  # Create a table
  my $sql = Mojo::SQLite->new(Mojo::URL->new->scheme('file')->path($filename));
  $sql->db->query('create table names (id integer primary key autoincrement, name text)');

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
  $db->query('insert into names (name) values (?)', 'Daniel');
  say $db->query('select last_insert_rowid() as id')->hash->{id};

  # Select one row at a time
  my $results = $db->query('select * from names');
  while (my $next = $results->hash) {
    say $next->{name};
  }

  # Select all rows
  $db->query('select * from names')
    ->hashes->map(sub { $_->{name} })->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::SQLite> is a tiny wrapper around L<DBD::SQLite> that makes
L<SQLite|https://www.sqlite.org/> a lot of fun to use with the
L<Mojolicious|https://mojolico.us> real-time web framework.

Database and statement handles are cached automatically, so they can be reused
transparently to increase performance. And you can handle connection timeouts
gracefully by holding on to them only for short amounts of time.

  use Mojolicious::Lite;
  use Mojo::SQLite;
  use Mojo::URL;

  helper sqlite =>
    sub { state $sql = Mojo::SQLite->new(Mojo::URL->new->path(shift->config('sqlite_filename')) };

  get '/' => sub {
    my $c  = shift;
    my $db = $c->sqlite->db;
    $c->render(json => $db->query('select datetime("now","localtime") as time')->hash);
  };

  app->start;

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

While passing a file path of C<:memory:> (or a custom L</"dsn"> with
C<mode=memory>) will create a temporary database, in-memory databases cannot be
shared between connections, so subsequent calls to L</"db"> may return
connections to completely different databases. For a temporary database that
can be shared between connections and processes, pass a file path of C<:temp:>
to store the database in a temporary file (this is the default).

=head1 EVENTS

L<Mojo::SQLite> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 connection

  $sql->on(connection => sub {
    my ($sql, $dbh) = @_;
    ...
  });

Emitted when a new database connection has been established.

=head1 ATTRIBUTES

L<Mojo::SQLite> implements the following attributes.

=head2 dsn

  my $dsn = $sql->dsn;
  $sql    = $sql->dsn('dbi:SQLite:uri=file:foo.db');

Data source name, defaults to a C<dbi:SQLite:uri=> followed by a URI to a
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

Options for database handles, defaults to activating C<AutoCommit>,
C<AutoInactiveDestroy> as well as C<RaiseError> and deactivating C<PrintError>.
Note that C<AutoCommit> and C<RaiseError> are considered mandatory, so
deactivating them would be very dangerous. L<DBD::SQLite> specific option
C<sqlite_unicode> is also set by default.

=head1 METHODS

L<Mojo::SQLite> inherits all methods from L<Mojo::EventEmitter> and implements
the following new ones.

=head2 db

  my $db = $sql->db;

Get L<Mojo::SQLite::Database> object for a cached or newly established database
connection. The L<DBD::SQLite> database handle will be automatically cached
again when that object is destroyed, so you can handle connection timeouts
gracefully by holding on to it only for short amounts of time.

  # Add up all the money
  say $sql->db->query('select * from accounts')
    ->hashes->reduce(sub { $a->{money} + $b->{money} });

=head2 from_string

  $sql = $sql->from_string('file:test.db');

Parse configuration from connection string. Connection strings are parsed as
URIs, so you should construct them using a module like L<Mojo::URL> or
L<URI::file>. The scheme and hostname are optional, but if specified must be
C<file> and C<localhost> respectively.

  # Absolute filename
  $sql->from_string('file:///home/fred/data.db');
  $sql->from_string('file://localhost/home/fred/data.db');
  $sql->from_string('file:/home/fred/data.db');
  $sql->from_string('///home/fred/data.db');
  $sql->from_string('//localhost/home/fred/data.db');
  $sql->from_string('/home/fred/data.db');

  # Relative to current directory
  $sql->from_string('file:data.db');
  $sql->from_string('data.db');

  # Connection string must be a valid URI
  $sql->from_string(Mojo::URL->new->scheme('file')->path($filename));
  $sql->from_string(URI::file->new($filename));

  # Temporary file database (default)
  $sql->from_string(':temp:');

  # In-memory temporary database (single connection only)
  my $db = $sql->from_string(':memory:')->db;

  # Additional options
  $sql->from_string('data.db?PrintError=1&sqlite_allow_multiple_statements=1');
  $sql->from_string(Mojo::URL->new->scheme('file')->path($filename)->query(PrintError => 1));

=head2 new

  my $sql = Mojo::SQLite->new;
  my $sql = Mojo::SQLite->new('test.db');

Construct a new L<Mojo::SQLite> object and parse connection string with
L</"from_string"> if necessary.

  # Customize configuration further
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:uri=file:test.db?mode=memory');

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

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious>, L<DBD::SQLite>
