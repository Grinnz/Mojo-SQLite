use Mojo::Base -strict;

use Test::More;
use Mojo::SQLite;

# Connected
my $sql = Mojo::SQLite->new;
ok $sql->db->ping, 'connected';

# Blocking select
is_deeply $sql->db->query('select 1 as one, 2 as two, 3 as three')->hash,
  {one => 1, two => 2, three => 3}, 'right structure';

# Connection cache
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

# Statement cache
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

# Fork-safety
$dbh = $sql->db->dbh;
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
};
$sql->unsubscribe('connection');

# Blocking error
eval { $sql->db->query('does_not_exist') };
like $@, qr/does_not_exist/, 'right error';

done_testing();
