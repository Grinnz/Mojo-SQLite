use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;
use Mojo::SQLite;
use Mojo::IOLoop;
use Mojo::JSON 'true';
use DBI ':sql_types';
use Mojo::Util 'encode';

# Connected
my $sql = Mojo::SQLite->new;
ok $sql->db->ping, 'connected';

# Blocking select
is_deeply $sql->db->query('select 1 as one, 2 as two, 3 as three')->hash,
  {one => 1, two => 2, three => 3}, 'right structure';

# Non-blocking select
{
  my ($fail, $result);
  my $db = $sql->db;
  $db->query(
    'select 1 as one, 2 as two, 3 as three' => sub {
      my ($db, $err, $results) = @_;
      $fail   = $err;
      $result = $results->hash;
      Mojo::IOLoop->stop;
    }
  );
  Mojo::IOLoop->start;
  ok !$fail, 'no error';
  is_deeply $result, {one => 1, two => 2, three => 3}, 'right structure';
}

# Concurrent non-blocking selects
{
  my ($fail, $result);
  Mojo::IOLoop->delay(
    sub {
      my $delay = shift;
      $sql->db->query('select 1 as one' => $delay->begin);
      $sql->db->query('select 2 as two' => $delay->begin);
      $sql->db->query('select 2 as two' => $delay->begin);
    },
    sub {
      my ($delay, $err_one, $one, $err_two, $two, $err_again, $again) = @_;
      $fail = $err_one || $err_two || $err_again;
      $result
        = [$one->hashes->first, $two->hashes->first, $again->hashes->first];
    }
  )->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {two => 2}, {two => 2}], 'right structure';
}

# Sequential non-blocking selects
{
  my ($fail, $result);
  my $db = $sql->db;
  Mojo::IOLoop->delay(
    sub {
      my $delay = shift;
      $db->query('select 1 as one' => $delay->begin);
    },
    sub {
      my ($delay, $err, $one) = @_;
      $fail = $err;
      push @$result, $one->hashes->first;
      $db->query('select 1 as one' => $delay->begin);
    },
    sub {
      my ($delay, $err, $again) = @_;
      $fail ||= $err;
      push @$result, $again->hashes->first;
      $db->query('select 2 as two' => $delay->begin);
    },
    sub {
      my ($delay, $err, $two) = @_;
      $fail ||= $err;
      push @$result, $two->hashes->first;
    }
  )->wait;
  ok !$fail, 'no error';
  is_deeply $result, [{one => 1}, {one => 1}, {two => 2}], 'right structure';
}

# Connection cache
{
  is $sql->max_connections, 5, 'right default';
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
}

# Statement cache
{
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
}

# Bind types
{
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
}

# JSON
{
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
  my $results = $db->query('select ? as foo', undef);
  is_deeply $results->expand(json => 'foo')->array, [undef], 'right structure';
  is_deeply $results->expand(json => 'foo')->array, undef, 'no more results';
  is_deeply $db->query('select ? as unicode', {json => {'☃' => '♥'}})
    ->expand(json => 'unicode')->hash, {unicode => {'☃' => '♥'}}, 'right structure';
  is_deeply $db->query("select json_object('☃', ?) as unicode", '♥')
    ->expand(json => 'unicode')->hash, {unicode => {'☃' => '♥'}}, 'right structure';
}

# Fork-safety
{
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
}

# Notifications
{
  my $db = $sql->db->notification_poll_interval(0.1);
  ok !$db->is_listening, 'not listening';
  ok $db->listen('dbtest')->is_listening, 'listening';
  my $db2 = $sql->db->notification_poll_interval(0.1)->listen('dbtest');
  my @notifications;
  Mojo::IOLoop->delay(
    sub {
      my $delay = shift;
      $db->once(notification => $delay->begin);
      $db2->once(notification => $delay->begin);
      Mojo::IOLoop->next_tick(sub { $db2->notify(dbtest => 'foo') });
    },
    sub {
      my ($delay, $name, $payload, $name2, $payload2) = @_;
      push @notifications, [$name, $payload], [$name2, $payload2];
      $db->once(notification => $delay->begin);
      $db2->unlisten('dbtest');
      Mojo::IOLoop->next_tick(sub { $sql->db->notify('dbtest') });
    },
    sub {
      my ($delay, $name, $payload) = @_;
      push @notifications, [$name, $payload];
      $db2->listen('dbtest2')->once(notification => $delay->begin);
      Mojo::IOLoop->next_tick(sub { $db2->notify(dbtest2 => 'bar') });
    },
    sub {
      my ($delay, $name, $payload) = @_;
      push @notifications, [$name, $payload];
      $db2->once(notification => $delay->begin);
      my $tx = $db2->begin;
      Mojo::IOLoop->next_tick(
        sub {
          $db2->notify(dbtest2 => 'baz');
          $tx->commit;
        }
      );
    },
    sub {
      my ($delay, $name, $payload) = @_;
      push @notifications, [$name, $payload];
    }
  )->wait;
  ok !$db->unlisten('dbtest')->is_listening, 'not listening';
  ok !$db2->unlisten('*')->is_listening,     'not listening';
  is $notifications[0][0], 'dbtest',  'right channel name';
  is $notifications[0][1], 'foo',     'right payload';
  is $notifications[1][0], 'dbtest',  'right channel name';
  is $notifications[1][1], 'foo',     'right payload';
  is $notifications[2][0], 'dbtest',  'right channel name';
  is $notifications[2][1], '',        'no payload';
  is $notifications[3][0], 'dbtest2', 'right channel name';
  is $notifications[3][1], 'bar',     'no payload';
  is $notifications[4][0], 'dbtest2', 'right channel name';
  is $notifications[4][1], 'baz',     'no payload';
  is $notifications[5], undef, 'no more notifications';

  # Stop listening for all notifications
  ok !$db->is_listening, 'not listening';
  ok $db->listen('dbtest')->listen('dbtest2')->unlisten('dbtest2')->is_listening,
    'listening';
  ok !$db->unlisten('*')->is_listening, 'not listening';

  # Connection close while listening for notifications
  {
    ok $db->listen('dbtest')->is_listening, 'listening';
    my $close = 0;
    $db->on(close => sub { $close++ });
    $db->dbh->disconnect;
    Mojo::IOLoop->start;
    is $close, 1, 'close event has been emitted once';
  }
}

# Blocking error
eval { $sql->db->query('does_not_exist') };
like $@, qr/does_not_exist/, 'right error';

# Non-blocking error
{
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
}

done_testing();
