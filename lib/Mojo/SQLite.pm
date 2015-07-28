package Mojo::SQLite;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;
use Mojo::SQLite::Database;
use Mojo::SQLite::Migrations;
use Mojo::URL;
use Scalar::Util 'weaken';

our $VERSION = '0.001';

has dsn             => 'dbi:SQLite:uri=file::memory:';
has max_connections => 1;
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
    RaiseError          => 1
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
  croak qq{Invalid SQLite connection string "$str"}
    unless $url->protocol eq 'file' and ($url->host // '') =~ /^(localhost)?\z/;

  # Database file
  my $uri = $url->clone->query('')->fragment(undef)->userinfo(undef)->port(undef);
  my $dsn = "dbi:SQLite:uri=$uri";

  # Options
  my $hash = $url->query->to_hash;
  @{$self->options}{keys %$hash} = values %$hash;

  return $self->dsn($dsn);
}

sub new { @_ > 1 ? shift->SUPER::new->from_string(@_) : shift->SUPER::new }

sub _dequeue {
  my $self = shift;
  while (my $dbh = shift @{$self->{queue} || []}) { return $dbh if $dbh->ping }
  my $dbh = DBI->connect(map { $self->$_ } qw(dsn username password options));
  $self->emit(connection => $dbh);
  return $dbh;
}

sub _enqueue {
  my ($self, $dbh) = @_;
  my $queue = $self->{queue} ||= [];
  push @$queue, $dbh if $dbh->{Active};
  shift @$queue while @$queue > $self->max_connections;
}

1;

=head1 NAME

Mojo::SQLite - A tiny Mojolicious wrapper for SQLite

  use Mojo::SQLite;

  # Create a table
  my $sql = Mojo::SQLite->new('file:test.db');
  $sql->db->query('create table names (id integer primary key autoincrement, name text)');

  # Insert a few rows
  my $db = $sql->db;
  $sql->query('insert into names (name) values (?)', 'Sara');
  $sql->query('insert into names (name) values (?)', 'Stefan');

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

  helper sqlite =>
    sub { state $sql = Mojo::SQLite->new('file:///home/fred/data.db') };

  get '/' => sub {
    my $c  = shift;
    my $db = $c->sqlite->db;
    $c->render(json => $db->query('select datetime("now","localtime") as time')->hash);
  };

  app->start;

All I/O and queries are performed synchronously. However, connecting in WAL
mode means that multiple processes can access the same SQLite database
concurrently, though only one can write at a time.

  # Performed concurrently
  my $pid = fork || die $!;
  say $sql->db->query('select datetime("now","localtime") as time')->hash->{time};
  exit unless $pid;

All cached database handles will be reset automatically if a new process has
been forked, this allows multiple processes to share the same L<Mojo::SQLite>
object safely.

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

Data source name, defaults to C<dbi:SQLite:uri=file::memory:>.

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

Options for database handles, defaults to activating C<AutoCommit>,
C<AutoInactiveDestroy> as well as C<RaiseError> and deactivating C<PrintError>.
Note that C<AutoCommit> and C<RaiseError> are considered mandatory, so
deactivating them would be very dangerous.

=head2 password

  my $password = $sql->password;
  $sql         = $sql->password('s3cret');

Database password, ignored by SQLite.

=head2 username

  my $username = $sql->username;
  $sql         = $sql->username('dbook');

Database username, ignored by SQLite.

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

Parse configuration from connection string.

  # Absolute filename
  $sql->from_string('file:///home/fred/data.db');

  # Relative to current directory
  $sql->from_string('file:data.db');

  # In-memory temporary database
  $sql->from_string('file::memory:');

  # Additional options
  $sql->from_string('file:data.db?PrintError=1&sqlite_allow_multiple_statements=1');

=head2 new

  my $sql = Mojo::SQLite->new;
  my $sql = Mojo::SQLite->new('file:test.db');

Construct a new L<Mojo::SQLite> object and parse connection string with
L</"from_string"> if necessary.

  # Customize configuration further
  my $sql = Mojo::SQLite->new->dsn('dbi:SQLite:dbname=test.db?mode=memory');

=head1 REFERENCE

This is the class hierarchy of the L<Mojo::SQLite> distribution.

=over 2

=item * L<Mojo::SQLite>

=item * L<Mojo::SQLite::Database>

=item * L<Mojo::SQLite::Migrations>

=item * L<Mojo::SQLite::Results>

=item * L<Mojo::SQLite::Transaction>

=back

=head1 DESCRIPTION

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book, C<dbook@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious>
