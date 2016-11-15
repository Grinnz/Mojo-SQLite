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

done_testing();
