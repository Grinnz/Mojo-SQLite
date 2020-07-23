use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;

use Mojo::SQLite;
use Mojolicious::Lite;
use Scalar::Util 'refaddr';
use Test::Mojo;

helper sqlite => sub { state $sql = Mojo::SQLite->new };

app->sqlite->db->query('create table if not exists app_test (stuff text)');
app->sqlite->db->query('insert into app_test values (?)', 'I ♥ Mojolicious!');

get '/blocking' => sub {
  my $c  = shift;
  my $db = $c->sqlite->db;
  $c->res->headers->header('X-Ref' => refaddr $db->dbh);
  $c->render(text => $db->query('select * from app_test')->hash->{stuff});
};

my $t = Test::Mojo->new;

subtest 'Make sure migrations are not served as static files' => sub {
  $t->get_ok('/app_test')->status_is(404);
};

subtest 'Blocking select (with connection reuse)' => sub {
  $t->get_ok('/blocking')->status_is(200)->content_is('I ♥ Mojolicious!');
  my $ref = $t->tx->res->headers->header('X-Ref');
  $t->get_ok('/blocking')->status_is(200)->header_is('X-Ref', $ref)
    ->content_is('I ♥ Mojolicious!');
  $t->app->sqlite->db->query('drop table app_test');
};

done_testing();
