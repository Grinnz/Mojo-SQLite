package Mojo::SQLite::Database;
use Mojo::Base -base;

use Carp qw(croak shortmess);
use DBI 'SQL_VARCHAR';
use Mojo::JSON 'to_json';
use Mojo::Promise;
use Mojo::SQLite::Results;
use Mojo::SQLite::Transaction;
use Mojo::Util 'monkey_patch';

our $VERSION = '3.009';

our @CARP_NOT = qw(Mojo::SQLite::Migrations);

has [qw(dbh sqlite)];
has results_class => 'Mojo::SQLite::Results';

for my $name (qw(delete insert select update)) {
  monkey_patch __PACKAGE__, $name, sub {
    my ($self, @cb) = (shift, ref $_[-1] eq 'CODE' ? pop : ());
    return $self->query($self->sqlite->abstract->$name(@_), @cb);
  };
  monkey_patch __PACKAGE__, "${name}_p", sub {
    my $self = shift;
    return $self->query_p($self->sqlite->abstract->$name(@_));
  };
}

sub DESTROY {
  my $self = shift;

  # Supported on Perl 5.14+
  return() if defined ${^GLOBAL_PHASE} && ${^GLOBAL_PHASE} eq 'DESTRUCT';

  return() unless (my $sql = $self->sqlite) && (my $dbh = $self->dbh);
  $sql->_enqueue($dbh);
}

sub begin {
  my ($self, $behavior) = @_;
  return Mojo::SQLite::Transaction->new(db => $self, behavior => $behavior);
}

sub disconnect {
  my $self = shift;
  $self->dbh->disconnect;
}

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $dbh = $self->dbh;

  my $prev_h = $dbh->{HandleError};
  # Better context for error messages
  local $dbh->{HandleError} = sub { $_[0] = shortmess $_[0]; ($prev_h and $prev_h->(@_)) ? 1 : 0 };

  my ($sth, $errored, $error);
  {
    local $@;
    unless (eval {
      # If RaiseError has been disabled, we might not get a handle
      if (defined($sth = $dbh->prepare_cached($query, undef, 3))) {
        _bind_params($sth, @_);
        $sth->execute;
      }
      1;
    }) { $errored = 1; $error = $@ }
  }

  die $error if $errored and !$cb; # bail out for errored "blocking" queries

  # We won't have a statement handle if prepare failed in a "non-blocking"
  # query or with RaiseError disabled
  my $results;
  if (defined $sth) {
    $results = $self->results_class->new(db => $self, sth => $sth);
    $results->{last_insert_id} = $dbh->{private_mojo_last_insert_id};
  }

  return $results unless $cb; # blocking

  # Still blocking, but call the callback on the next tick
  $error = $dbh->err ? $dbh->errstr : $errored ? ($error || 'Error running SQLite query') : undef;
  require Mojo::IOLoop;
  Mojo::IOLoop->next_tick(sub { $self->$cb($error, $results) });
  return $self;
}

sub query_p {
  my $self    = shift;
  my $promise = Mojo::Promise->new;
  $self->query(@_ => sub { $_[1] ? $promise->reject($_[1]) : $promise->resolve($_[2]) });
  return $promise;
}

sub tables {
  my @tables = shift->dbh->tables(undef, undef, undef, 'TABLE,VIEW,LOCAL TEMPORARY');
  my %names; # Deduplicate returned temporary table indexes
  return [grep { !$names{$_}++ } @tables];
}

sub _bind_params {
  my $sth = shift;
  return $sth unless @_;
  foreach my $i (0..$#_) {
    my $param = $_[$i];
    if (ref $param eq 'HASH') {
      if (exists $param->{type} && exists $param->{value}) {
        $sth->bind_param($i+1, $param->{value}, $param->{type});
      } elsif (exists $param->{json}) {
        $sth->bind_param($i+1, to_json($param->{json}), SQL_VARCHAR);
      } elsif (exists $param->{-json}) {
        $sth->bind_param($i+1, to_json($param->{-json}), SQL_VARCHAR);
      } else {
        croak qq{Unknown parameter hashref (no "type"/"value", "json" or "-json")};
      }
    } else {
      $sth->bind_param($i+1, $param);
    }
  }
  return $sth;
}

1;

=encoding utf8

=head1 NAME

Mojo::SQLite::Database - Database

=head1 SYNOPSIS

  use Mojo::SQLite::Database;

  my $db = Mojo::SQLite::Database->new(sqlite => $sql, dbh => $dbh);
  $db->query('select * from foo')
    ->hashes->map(sub { $_->{bar} })->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::SQLite::Database> is a container for L<DBD::SQLite> database handles
used by L<Mojo::SQLite>.

=head1 ATTRIBUTES

L<Mojo::SQLite::Database> implements the following attributes.

=head2 dbh

  my $dbh = $db->dbh;
  $db     = $db->dbh($dbh);

L<DBD::SQLite> database handle used for all queries.

  # Use DBI utility methods
  my $quoted = $db->dbh->quote_identifier('foo.bar');

=head2 results_class

  my $class = $db->results_class;
  $db       = $db->results_class('MyApp::Results');

Class to be used by L</"query">, defaults to L<Mojo::SQLite::Results>. Note
that this class needs to have already been loaded before L</"query"> is called.

=head2 sqlite

  my $sql = $db->sqlite;
  $db     = $db->sqlite(Mojo::SQLite->new);

L<Mojo::SQLite> object this database belongs to.

=head1 METHODS

L<Mojo::SQLite::Database> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 begin

  my $tx = $db->begin;
  my $tx = $db->begin('exclusive');

Begin transaction and return L<Mojo::SQLite::Transaction> object, which will
automatically roll back the transaction unless
L<Mojo::SQLite::Transaction/"commit"> has been called before it is destroyed.

  # Insert rows in a transaction
  eval {
    my $tx = $db->begin;
    $db->insert('frameworks', {name => 'Catalyst'});
    $db->insert('frameworks', {name => 'Mojolicious'});
    $tx->commit;
  };
  say $@ if $@;

A transaction locking behavior of C<deferred>, C<immediate>, or C<exclusive>
may optionally be passed; the default in L<DBD::SQLite> is currently
C<immediate>. See L<DBD::SQLite/"Transaction and Database Locking"> and
L<https://sqlite.org/lang_transaction.html> for more details.

=head2 delete

  my $results = $db->delete($table, \%where);

Generate a C<DELETE> statement with L<Mojo::SQLite/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback for API compatibility with L<Mojo::Pg>; the query is still
executed in a blocking manner.

  $db->delete(some_table => sub ($db, $err, $results) {
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<delete> method of
L<SQL::Abstract>.

  # "delete from some_table"
  $db->delete('some_table');

  # "delete from some_table where foo = 'bar'"
  $db->delete('some_table', {foo => 'bar'});

  # "delete from some_table where foo like '%test%'"
  $db->delete('some_table', {foo => {-like => '%test%'}});

=head2 delete_p

  my $promise = $db->delete_p($table, \%where, \%options);

Same as L</"delete"> but returns a L<Mojo::Promise> object instead of accepting
a callback. For API compatibility with L<Mojo::Pg>; the query is still executed
in a blocking manner.

  $db->delete_p('some_table')->then(sub ($results) {
    ...
  })->catch(sub ($err) {
    ...
  })->wait;

=head2 disconnect

  $db->disconnect;

Disconnect L</"dbh"> and prevent it from getting reused.

=head2 insert

  my $results = $db->insert($table, \@values || \%fieldvals, \%options);

Generate an C<INSERT> statement with L<Mojo::SQLite/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback for API compatibility with L<Mojo::Pg>; the query is still
executed in a blocking manner.

  $db->insert(some_table => {foo => 'bar'} => sub ($db, $err, $results) {
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<insert> method of
L<SQL::Abstract>.

  # "insert into some_table (foo, baz) values ('bar', 'yada')"
  $db->insert('some_table', {foo => 'bar', baz => 'yada'});

=head2 insert_p

  my $promise = $db->insert_p($table, \@values || \%fieldvals, \%options);

Same as L</"insert"> but returns a L<Mojo::Promise> object instead of accepting
a callback. For API compatibility with L<Mojo::Pg>; the query is still executed
in a blocking manner.

  $db->insert_p(some_table => {foo => 'bar'})->then(sub ($results) {
    ...
  })->catch(sub ($err) {
    ...
  })->wait;

=head2 ping

  my $bool = $db->ping;

Check database connection.

=head2 query

  my $results = $db->query('select * from foo');
  my $results = $db->query('insert into foo values (?, ?, ?)', @values);
  my $results = $db->query('select ? as img', {type => SQL_BLOB, value => slurp 'img.jpg'});
  my $results = $db->query('select ? as foo', {json => {bar => 'baz'}});

Execute a blocking L<SQL|http://www.postgresql.org/docs/current/static/sql.html>
statement and return a results object based on L</"results_class"> (which is
usually L<Mojo::SQLite::Results>) with the query results. The L<DBD::SQLite>
statement handle will be automatically reused when it is not active anymore, to
increase the performance of future queries. You can also append a callback for
API compatibility with L<Mojo::Pg>; the query is still executed in a blocking
manner.

  $db->query('insert into foo values (?, ?, ?)' => @values => sub ($db, $err, $results) {
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Hash reference arguments containing C<type> and C<value> elements will use the
specified bind type for the parameter, using types from L<DBI/"DBI Constants">;
see L<DBD::SQLite/"Blobs"> and the subsequent section for more information.

Hash reference arguments containing a value named C<json> or C<-json> will be
encoded to L<JSON text|http://sqlite.org/json1.html> with
L<Mojo::JSON/"to_json">. To accomplish the reverse, you can use the method
L<Mojo::SQLite::Results/"expand"> to decode JSON text fields to Perl values
with L<Mojo::JSON/"from_json">.

  # "I ♥ SQLite!"
  $db->query('select ? as foo', {json => {bar => 'I ♥ SQLite!'}})
    ->expand(json => 'foo')->hash->{foo}{bar};

=head2 query_p

  my $promise = $db->query_p('SELECT * FROM foo');

Same as L</"query"> but returns a L<Mojo::Promise> object instead of accepting
a callback. For API compatibility with L<Mojo::Pg>; the query is still executed
in a blocking manner.

  $db->query_p('INSERT INTO foo VALUES (?, ?, ?)' => @values)->then(sub ($results) {
    ...
  })->catch(sub ($err) {
    ...
  })->wait;

=head2 select

  my $results = $db->select($source, $fields, $where, $order);

Generate a C<SELECT> statement with L<Mojo::SQLite/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback for API compatibility with L<Mojo::Pg>; the query is still
executed in a blocking manner.

  $db->select(some_table => ['foo'] => {bar => 'yada'} => sub ($db, $err, $results) {
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<select> method of
L<SQL::Abstract>.

  # "select * from some_table"
  $db->select('some_table');

  # "select id, foo from some_table"
  $db->select('some_table', ['id', 'foo']);

  # "select * from some_table where foo = 'bar'"
  $db->select('some_table', undef, {foo => 'bar'});

  # "select * from some_table where foo = 'bar' order by id desc"
  $db->select('some_table', undef, {foo => 'bar'}, {-desc => 'id'});

  # "select * from some_table where foo like '%test%'"
  $db->select('some_table', undef, {foo => {-like => '%test%'}});

=head2 select_p

  my $promise = $db->select_p($source, $fields, $where, \%options);

Same as L</"select"> but returns a L<Mojo::Promise> object instead of accepting
a callback. For API compatibility with L<Mojo::Pg>; the query is still executed
in a blocking manner.

  $db->select_p(some_table => ['foo'] => {bar => 'yada'})->then(sub ($results) {
    ...
  })->catch(sub ($err) {
    ...
  })->wait;

=head2 tables

  my $tables = $db->tables;

Return table and view names for this database, that are visible to the current
user and not internal, as an array reference. Names will be quoted and prefixed
by a schema name of C<"main"> for standard tables, C<"temp"> for temporary
tables, and the appropriate schema name for
L<attached databases|http://sqlite.org/lang_attach.html>.

  # Names of all tables
  say for @{$db->tables};

=head2 update

  my $results = $db->update($table, \%fieldvals, \%where);

Generate an C<UPDATE> statement with L<Mojo::SQLite/"abstract"> (usually an
L<SQL::Abstract::Pg> object) and execute it with L</"query">. You can also
append a callback for API compatibility with L<Mojo::Pg>; the query is still
executed in a blocking manner.

  $db->update(some_table => {foo => 'baz'} => {foo => 'bar'} => sub ($db, $err, $results) {
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Use all the same argument variations you would pass to the C<update> method of
L<SQL::Abstract>.

  # "update some_table set foo = 'bar' where id = 23"
  $db->update('some_table', {foo => 'bar'}, {id => 23});

  # "update some_table set foo = 'bar' where foo like '%test%'"
  $db->update('some_table', {foo => 'bar'}, {foo => {-like => '%test%'}});

=head2 update_p

  my $promise = $db->update_p($table, \%fieldvals, \%where, \%options);

Same as L</"update"> but returns a L<Mojo::Promise> object instead of accepting
a callback. For API compatibility with L<Mojo::Pg>; the query is still executed
in a blocking manner.

  $db->update_p(some_table => {foo => 'baz'} => {foo => 'bar'})->then(sub ($results) {
    ...
  })->catch(sub ($err) {
    ...
  })->wait;

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
