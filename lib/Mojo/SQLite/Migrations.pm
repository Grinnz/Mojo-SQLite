package Mojo::SQLite::Migrations;
use Mojo::Base -base;

use Carp 'croak';
use Mojo::File 'path';
use Mojo::Loader 'data_section';
use Mojo::Util 'decode';

use constant DEBUG => $ENV{MOJO_MIGRATIONS_DEBUG} || 0;

our $VERSION = '3.009';

has name => 'migrations';
has sqlite => undef, weak => 1;

sub active { $_[0]->_active($_[0]->sqlite->db) }

sub from_data {
  my ($self, $class, $name) = @_;
  return $self->from_string(
    data_section($class //= caller, $name // $self->name));
}

sub from_file { shift->from_string(decode 'UTF-8', path(pop)->slurp) }

sub from_string {
  my ($self, $sql) = @_;

  my ($version, $way);
  my $migrations = $self->{migrations} = {up => {}, down => {}};
  for my $line (split "\n", $sql // '') {
    ($version, $way) = ($1, lc $2) if $line =~ /^\s*--\s*(\d+)\s*(up|down)/i;
    $migrations->{$way}{$version} .= "$line\n" if $version;
  }

  return $self;
}

sub latest {
  (sort { $a <=> $b } keys %{shift->{migrations}{up}})[-1] || 0;
}

sub migrate {
  my ($self, $target) = @_;

  # Unknown version
  my $latest = $self->latest;
  $target //= $latest;
  my ($up, $down) = @{$self->{migrations}}{qw(up down)};
  croak "Version $target has no migration" if $target != 0 && !$up->{$target};

  # Already the right version (make sure migrations table exists)
  my $db = $self->sqlite->db;
  return $self if $self->_active($db, 0) == $target;

  # Lock migrations table and check version again
  my $tx = $db->begin('exclusive');
  return $self if (my $active = $self->_active($db, 1)) == $target;

  # Newer version
  croak "Active version $active is greater than the latest version $latest"
    if $active > $latest;

  my $query = $self->sql_for($active, $target);
  warn "-- Migrate ($active -> $target)\n$query\n" if DEBUG;
  local $db->dbh->{sqlite_allow_multiple_statements} = 1;

  # Disable update hook during migrations
  my $hook = $db->dbh->sqlite_update_hook(undef);

  # Catch the error so we can croak it  
  my ($errored, $error, $result);
  {
    local $@;
    eval { $result = $db->dbh->do($query); 1 } or $errored = 1;
    $error = $@ if $errored;
  }
  
  # Re-enable update hook
  $db->dbh->sqlite_update_hook($hook);
  
  croak $error if $errored;
  return $self unless defined $result; # RaiseError disabled
  
  $db->query('update mojo_migrations set version = ? where name = ?',
    $target, $self->name) and $tx->commit;

  return $self;
}

sub sql_for {
  my ($self, $from, $to) = @_;

  # Up
  my ($up, $down) = @{$self->{migrations}}{qw(up down)};
  if ($from < $to) {
    my @up = grep { $_ <= $to && $_ > $from } keys %$up;
    return join '', @$up{sort { $a <=> $b } @up};
  }

  # Down
  my @down = grep { $_ > $to && $_ <= $from } keys %$down;
  return join '', @$down{reverse sort { $a <=> $b } @down};
}

sub _active {
  my ($self, $db, $create) = @_;

  my $name = $self->name;
  my $results;
  {
    local $db->dbh->{RaiseError} = 0;
    my $query = 'select version from mojo_migrations where name = ?';
    $results = $db->query($query, $name);
  }
  my $next = $results ? $results->array : undef;
  if ($next || !$create) { return $next->[0] || 0 }

  $db->query(
    'create table if not exists mojo_migrations (
       name    text not null primary key,
       version integer not null check (version >= 0)
     )'
  ) if !$results or $results->sth->err;
  $db->query('insert into mojo_migrations values (?, ?)', $name, 0);

  return 0;
}

1;

=encoding utf8

=head1 NAME

Mojo::SQLite::Migrations - Migrations

=head1 SYNOPSIS

  use Mojo::SQLite::Migrations;

  my $migrations = Mojo::SQLite::Migrations->new(sqlite => $sql);
  $migrations->from_file('/home/dbook/migrations.sql')->migrate;

=head1 DESCRIPTION

L<Mojo::SQLite::Migrations> is used by L<Mojo::SQLite> to allow database
schemas to evolve easily over time. A migration file is just a collection of
sql blocks, with one or more statements, separated by comments of the form
C<-- VERSION UP/DOWN>.

  -- 1 up
  create table messages (message text);
  insert into messages values ('I ♥ Mojolicious!');
  -- 1 down
  drop table messages;

  -- 2 up (...you can comment freely here...)
  create table stuff (whatever integer);
  -- 2 down
  drop table stuff;

The idea is to let you migrate from any version, to any version, up and down.
Migrations are very safe, because they are performed in transactions and only
one can be performed at a time. If a single statement fails, the whole
migration will fail and get rolled back. Every set of migrations has a
L</"name">, which is stored together with the currently active version in an
automatically created table named C<mojo_migrations>.

=head1 ATTRIBUTES

L<Mojo::SQLite::Migrations> implements the following attributes.

=head2 name

  my $name    = $migrations->name;
  $migrations = $migrations->name('foo');

Name for this set of migrations, defaults to C<migrations>.

=head2 sqlite

  my $sql     = $migrations->sqlite;
  $migrations = $migrations->sqlite(Mojo::SQLite->new);

L<Mojo::SQLite> object these migrations belong to. Note that this attribute is
weakened.

=head1 METHODS

L<Mojo::SQLite::Migrations> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 active

  my $version = $migrations->active;

Currently active version.

=head2 from_data

  $migrations = $migrations->from_data;
  $migrations = $migrations->from_data('main');
  $migrations = $migrations->from_data('main', 'file_name');

Extract migrations from a file in the DATA section of a class with
L<Mojo::Loader/"data_section">, defaults to using the caller class and
L</"name">.

  __DATA__
  @@ migrations
  -- 1 up
  create table messages (message text);
  insert into messages values ('I ♥ Mojolicious!');
  -- 1 down
  drop table messages;

=head2 from_file

  $migrations = $migrations->from_file('/home/dbook/migrations.sql');

Extract migrations from a file.

=head2 from_string

  $migrations = $migrations->from_string(
    '-- 1 up
     create table foo (bar integer);
     -- 1 down
     drop table foo;'
  );

Extract migrations from string.

=head2 latest

  my $version = $migrations->latest;

Latest version available.

=head2 migrate

  $migrations = $migrations->migrate;
  $migrations = $migrations->migrate(3);

Migrate from L</"active"> to a different version, up or down, defaults to using
L</"latest">. All version numbers need to be positive, with version C<0>
representing an empty database.

  # Reset database
  $migrations->migrate(0)->migrate;

=head2 sql_for

  my $sql = $migrations->sql_for(5, 10);

Get SQL to migrate from one version to another, up or down.

=head1 DEBUGGING

You can set the C<MOJO_MIGRATIONS_DEBUG> environment variable to get some
advanced diagnostics information printed to C<STDERR>.

  MOJO_MIGRATIONS_DEBUG=1

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book, C<dbook@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojo::SQLite>
