use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;
use Mojo::SQLite;
use Mojo::IOLoop;
use Mojo::JSON 'true';
use DBI ':sql_types';
use Mojo::Util 'encode';

my $sql = Mojo::SQLite->new;

subtest 'Connected' => sub {
  ok $sql->db->ping, 'connected';
};

subtest 'Blocking select' => sub {
  is_deeply $sql->db->query('select 1 as one, 2 as two, 3 as three')->hash,
    {one => 1, two => 2, three => 3}, 'right structure';
};

subtest 'Non-blocking select' => sub {
  my ($fail, $result);
  my $same;
  my $db = $sql->db;
  $db->query(
    'select 1 as one, 2 as two, 3 as three' => sub {
      my ($db, $err, $results) = @_;
      $fail   = $err;
      $result = $results->hash;
      $same   = $db->dbh eq $results->db->dbh;
      Mojo::IOLoop->stop;
    }
  );
  Mojo::IOLoop->start;
  ok $same, 'same database handles';
  ok !$fail, 'no error';
  is_deeply $result, {one => 1, two => 2, three => 3}, 'right structure';
};

subtest 'Concurrent non-blocking selects' => sub {
  my ($fail, $result);
  my $one   = $sql->db->query_p('select 1 as one');
  my $two   = $sql->db->query_p('select 2 as two');
  my $again = $sql->db->query_p('select 2 as two');
  Mojo::Promise->all($one, $again, $two)->then(sub {
    $result = [map { $_->[0]->hashes->first } @_];
  })->catch(sub {
    $fail = 1;
  })->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {two => 2}, {two => 2}], 'right structure';
};

subtest 'Sequential non-blocking selects' => sub {
  my ($fail, $result);
  my $db = $sql->db;
  $db->query_p('select 1 as one')->then(sub {
    push @$result, shift->hashes->first;
    $db->query_p('select 1 as one');
  })->then(sub {
    push @$result, shift->hashes->first;
    $db->query_p('select 2 as two');
  })->then(sub {
    push @$result, shift->hashes->first;
  })->catch(sub {
    $fail = 1;
  })->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {one => 1}, {two => 2}], 'right structure';
};

subtest 'Connection cache' => sub {
  is $sql->max_connections, 1, 'right default';
  my @dbhs = map { $_->dbh } $sql->db, $sql->db, $sql->db, $sql->db, $sql->db;
  is_deeply \@dbhs,
    [map { $_->dbh } $sql->db, $sql->db, $sql->db, $sql->db, $sql->db],
    'same database handles';
  @dbhs = ();
  my $dbh = $sql->max_connections(1)->db->dbh;
  is $sql->db->dbh, $dbh, 'same database handle';
  isnt $sql->db->dbh, $sql->db->dbh, 'different database handles';
  is $sql->db->dbh, $dbh, 'different database handles';
  $dbh = $sql->db->dbh;
  is $sql->db->dbh, $dbh, 'same database handle';
  $sql->db->disconnect;
  isnt $sql->db->dbh, $dbh, 'different database handles';
};

subtest 'Statement cache' => sub {
  my $db = $sql->db;
  my $sth = $db->query('select 3 as three')->sth;
  is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
  isnt $db->query('select 4 as four')->sth, $sth, 'different statement handles';
  is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
  undef $db;
  $db = $sql->db;
  my $results = $db->query('select 3 as three');
  is $results->sth, $sth, 'same statement handle';
  isnt $db->query('select 3 as three')->sth, $sth, 'different statement handles';
  $sth = $db->query('select 3 as three')->sth;
  is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
  isnt $db->query('select 5 as five')->sth, $sth, 'different statement handles';
  isnt $db->query('select 6 as six')->sth,  $sth, 'different statement handles';
  is $db->query('select 3 as three')->sth,  $sth, 'same statement handle';
};

subtest 'Connection reuse' => sub {
  my $db      = $sql->db;
  my $dbh     = $db->dbh;
  my $results = $db->query('select 1');
  undef $db;
  my $db2 = $sql->db;
  isnt $db2->dbh, $dbh, 'new database handle';
  undef $results;
  my $db3 = $sql->db;
  is $db3->dbh, $dbh, 'same database handle';
  $results = $db3->query('select 2');
  is $results->db->dbh, $dbh, 'same database handle';
  is $results->array->[0], 2, 'right result';
};

subtest 'Bind types' => sub {
  my $db = $sql->db;
  is_deeply $db->query('select ? as foo', {type => SQL_VARCHAR, value => 'bar'})
    ->hash, {foo => 'bar'}, 'right structure';
  is_deeply $db->query('select ? as foo', {type => SQL_INTEGER, value => 5})
    ->hash, {foo => 5}, 'right structure';
  is_deeply $db->query('select ? as foo', {type => SQL_REAL, value => 2.5})
    ->hash, {foo => 2.5}, 'right structure';
  is_deeply $db->query('select ? as foo', {type => SQL_VARCHAR, value => '☃♥'})
    ->hash, {foo => '☃♥'}, 'right structure';
  is_deeply $db->query('select ? as foo', {type => SQL_BLOB, value => encode 'UTF-8', '☃♥'})
    ->hash, {foo => encode 'UTF-8', '☃♥'}, 'right structure';
};

subtest 'JSON' => sub {
  my $db = $sql->db;
  is_deeply $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->hash, {foo => {bar => 'baz'}}, 'right structure';
  is_deeply $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->array, [{bar => 'baz'}], 'right structure';
  is_deeply $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->hashes->first, {foo => {bar => 'baz'}}, 'right structure';
  is_deeply $db->query('select ? as foo', {json => {bar => 'baz'}})
    ->expand(json => 'foo')->arrays->first, [{bar => 'baz'}], 'right structure';
  is_deeply $db->query('select ? as foo', {json => {bar => 'baz'}})->hash,
    {foo => '{"bar":"baz"}'}, 'right structure';
  is_deeply $db->query('select ? as foo', {json => \1})
    ->expand(json => 'foo')->hashes->first, {foo => true}, 'right structure';
  is_deeply $db->query('select ? as foo', undef)->expand(json => 'foo')->hash,
    {foo => undef}, 'right structure';
  is_deeply $db->query('select ? as foo', undef)->expand(json => 'foo')->array,
    [undef], 'right structure';
  my $results = $db->query('select ?', undef);
  my $name = $results->columns->[0];
  is_deeply $results->expand(json => $name)->array, [undef], 'right structure';
  is_deeply $results->expand(json => $name)->array, undef, 'no more results';
  is_deeply $db->query('select ? as unicode', {json => {'☃' => '♥'}})
    ->expand(json => 'unicode')->hash, {unicode => {'☃' => '♥'}}, 'right structure';
  is_deeply $db->query("select json_object('☃', ?) as unicode", '♥')
    ->expand(json => 'unicode')->hash, {unicode => {'☃' => '♥'}}, 'right structure';
  is_deeply $db->query('select ? as foo, ? as bar', {json => {baz => 'foo'}},
    {json => {baz => 'bar'}})->expand(json => 'foo')->hash,
    {foo => {baz => 'foo'}, bar => '{"baz":"bar"}'}, 'right structure';
  is_deeply $db->query('select ? as foo, ? as bar', {json => {baz => 'foo'}},
    {json => {baz => 'bar'}})->expand(json => ['foo','bar'])->hash,
    {foo => {baz => 'foo'}, bar => {baz => 'bar'}}, 'right structure';
};

subtest 'Fork-safety' => sub {
  my $dbh = $sql->db->dbh;
  my ($connections, $current) = @_;
  $sql->on(
    connection => sub {
      my ($sql, $dbh) = @_;
      $connections++;
      $current = $dbh;
    }
  );
  is $sql->db->dbh, $dbh, 'same database handle';
  ok !$connections, 'no new connections';
  {
    local $$ = -23;
    isnt $sql->db->dbh, $dbh,     'different database handles';
    is $sql->db->dbh,   $current, 'same database handle';
    is $connections, 1, 'one new connection';
  }
  $sql->unsubscribe('connection');
};

subtest 'Shared connection cache' => sub {
  my $sql2 = Mojo::SQLite->new($sql);
  is $sql2->parent, $sql, 'right parent';
  my $dbh = $sql->db->dbh;
  is $sql->db->dbh,  $dbh, 'same database handle';
  is $sql2->db->dbh, $dbh, 'same database handle';
  is $sql->db->dbh,  $dbh, 'same database handle';
  is $sql2->db->dbh, $dbh, 'same database handle';
  my $db = $sql->db;
  is_deeply $db->query('select 1 as one')->hashes->to_array, [{one => 1}],
    'right structure';
  $dbh = $db->dbh;
  $db->disconnect;
  $db = $sql2->db;
  is_deeply $db->query('select 1 as one')->hashes->to_array, [{one => 1}],
    'right structure';
  isnt $db->dbh, $dbh, 'different database handle';
};

subtest 'Blocking error' => sub {
  eval { $sql->db->query('does_not_exist') };
  like $@, qr/does_not_exist.*database\.t/s, 'right error';
};

subtest 'Non-blocking error' => sub {
  my ($fail, $result);
  my $db = $sql->db;
  $db->query(
    'does_not_exist' => sub {
      my ($db, $err, $results) = @_;
      ($fail, $result) = ($err, $results);
      Mojo::IOLoop->stop;
    }
  );
  Mojo::IOLoop->start;
  like $fail, qr/does_not_exist/, 'right error';
  is $db->dbh->errstr, $fail, 'same error';
};

subtest 'Error context' => sub {
  eval { $sql->db->query('select * from table_does_not_exist') };
  like $@, qr/database\.t/, 'right error';
};

subtest 'Double-quoted literal' => sub {
  ok !eval { $sql->db->query('select "does_not_exist"') }, 'no double-quoted string literals';
};

subtest 'WAL mode option' => sub {
  my $journal_mode = $sql->db->query('pragma journal_mode')->arrays->first->[0];
  is uc $journal_mode, 'WAL', 'right journal mode';
  
  my $sql = Mojo::SQLite->new;
  $sql->options->{no_wal} = 1;
  $journal_mode = $sql->db->query('pragma journal_mode')->arrays->first->[0];
  is uc $journal_mode, 'DELETE', 'right journal mode';
};

done_testing();
