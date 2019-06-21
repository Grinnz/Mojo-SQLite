use Mojo::Base -strict;

BEGIN { $ENV{MOJO_REACTOR} = 'Mojo::Reactor::Poll' }

use Test::More;
use Mojo::SQLite;
use Mojo::IOLoop;

my $sql = Mojo::SQLite->new;

{
  my $db = $sql->db;
  $db->query(
    'create table if not exists crud_test (
       id   integer primary key autoincrement,
       name text
     )'
  );

  # Create
  $db->insert('crud_test', {name => 'foo'});
  is_deeply $db->select('crud_test')->hashes->to_array,
    [{id => 1, name => 'foo'}], 'right structure';
  is $db->insert('crud_test', {name => 'bar'})->last_insert_id,
    2, 'right value';
  is_deeply $db->select('crud_test')->hashes->to_array,
    [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}], 'right structure';

  # Read
  is_deeply $db->select('crud_test')->hashes->to_array,
    [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}], 'right structure';
  is_deeply $db->select('crud_test', ['name'])->hashes->to_array,
    [{name => 'foo'}, {name => 'bar'}], 'right structure';
  is_deeply $db->select('crud_test', ['name'], {name => 'foo'})->hashes->to_array,
    [{name => 'foo'}], 'right structure';
  is_deeply $db->select('crud_test', ['name'], undef, {-desc => 'id'})
    ->hashes->to_array, [{name => 'bar'}, {name => 'foo'}], 'right structure';

  # Non-blocking read
  my $result;
  my $delay = Mojo::IOLoop->delay(sub { $result = pop->hashes->to_array });
  $db->select('crud_test', $delay->begin);
  $delay->wait;
  is_deeply $result, [{id => 1, name => 'foo'}, {id => 2, name => 'bar'}],
    'right structure';
  $result = undef;
  $delay = Mojo::IOLoop->delay(sub { $result = pop->hashes->to_array });
  $db->select('crud_test', undef, undef, {-desc => 'id'}, $delay->begin);
  $delay->wait;
  is_deeply $result, [{id => 2, name => 'bar'}, {id => 1, name => 'foo'}],
    'right structure';

  # Update
  $db->update('crud_test', {name => 'baz'}, {name => 'foo'});
  is_deeply $db->select('crud_test', undef, undef, {-asc => 'id'})
    ->hashes->to_array, [{id => 1, name => 'baz'}, {id => 2, name => 'bar'}],
    'right structure';

  # Delete
  $db->delete('crud_test', {name => 'baz'});
  is_deeply $db->select('crud_test', undef, undef, {-asc => 'id'})
    ->hashes->to_array, [{id => 2, name => 'bar'}], 'right structure';
  $db->delete('crud_test');
  is_deeply $db->select('crud_test')->hashes->to_array, [], 'right structure';

  # Quoting
  $db->query(
    'create table if not exists crud_test2 (
       id   integer primary key autoincrement,
       "t e s t" text
     )'
  );
  $db->insert('crud_test2',      {'t e s t' => 'foo'});
  $db->insert('main.crud_test2', {'t e s t' => 'bar'});
  is_deeply $db->select('main.crud_test2')->hashes->to_array,
    [{id => 1, 't e s t' => 'foo'}, {id => 2, 't e s t' => 'bar'}],
    'right structure';

  # Unresolved identifier
  is_deeply $db->select('main.crud_test2', undef, {'t e s t' => 'foo'})
    ->hashes->to_array, [{id => 1, 't e s t' => 'foo'}], 'rigth structure';
  ok !eval { $db->select('main.crud_test2', undef, {'test' => 'foo'}); 1 },
    'unknown column';
}

done_testing();
