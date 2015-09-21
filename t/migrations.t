use Mojo::Base -strict;

use Test::More;

use File::Spec::Functions 'catfile';
use File::Temp 'tempdir';
use FindBin;
use Mojo::SQLite;

my $tempdir = tempdir(CLEANUP => 1);
my $tempfile = catfile($tempdir, 'test.db');

# Clean up before start
my $sql = Mojo::SQLite->new->from_filename($tempfile);
$sql->db->query('drop table if exists mojo_migrations');

# Defaults
is $sql->migrations->name,   'migrations', 'right name';
is $sql->migrations->latest, 0,            'latest version is 0';
is $sql->migrations->active, 0,            'active version is 0';

# Create migrations table
ok !$sql->db->query(
  "select exists(
     select 1 from sqlite_master
     where type = 'table' and name = 'mojo_migrations'
   )"
)->array->[0], 'migrations table does not exist';
is $sql->migrations->migrate->active, 0, 'active version is 0';
ok $sql->db->query(
  "select exists(
     select 1 from sqlite_master
     where type = 'table' and name = 'mojo_migrations'
   )"
)->array->[0], 'migrations table exists';

# Migrations from DATA section
is $sql->migrations->from_data->latest, 0, 'latest version is 0';
is $sql->migrations->from_data(__PACKAGE__)->latest, 0, 'latest version is 0';
is $sql->migrations->name('test1')->from_data->latest, 10,
  'latest version is 10';
is $sql->migrations->name('test2')->from_data->latest, 2, 'latest version is 2';
is $sql->migrations->name('migrations')->from_data(__PACKAGE__, 'test1')
  ->latest, 10, 'latest version is 10';
is $sql->migrations->name('test2')->from_data(__PACKAGE__)->latest, 2,
  'latest version is 2';

# Different syntax variations
$sql->migrations->name('migrations_test')->from_string(<<EOF);
-- 1 up
create table if not exists migration_test_one (foo text);

-- 1down

  drop table if exists migration_test_one;

  -- 2 up

insert into migration_test_one values ('works ♥');
-- 2 down
delete from migration_test_one where foo = 'works ♥';
--
--  3 Up, create
--        another
--        table?
create table if not exists migration_test_two (bar text);
--3  DOWN
drop table if exists migration_test_two;

-- 10 up (not down)
insert into migration_test_two values ('works too');
-- 10 down (not up)
delete from migration_test_two where bar = 'works too';
EOF
is $sql->migrations->latest, 10, 'latest version is 10';
is $sql->migrations->active, 0,  'active version is 0';
is $sql->migrations->migrate->active, 10, 'active version is 10';
is_deeply $sql->db->query('select * from migration_test_one')->hash,
  {foo => 'works ♥'}, 'right structure';
is $sql->migrations->migrate->active, 10, 'active version is 10';
is $sql->migrations->migrate(1)->active, 1, 'active version is 1';
is $sql->db->query('select * from migration_test_one')->hash, undef,
  'no result';
is $sql->migrations->migrate(3)->active, 3, 'active version is 3';
is $sql->db->query('select * from migration_test_two')->hash, undef,
  'no result';
is $sql->migrations->migrate->active, 10, 'active version is 10';
is_deeply $sql->db->query('select * from migration_test_two')->hash,
  {bar => 'works too'}, 'right structure';
is $sql->migrations->migrate(0)->active, 0, 'active version is 0';

# Bad and concurrent migrations
my $sql2 = Mojo::SQLite->new->from_filename($tempfile);
$sql2->migrations->name('migrations_test2')
  ->from_file(catfile($FindBin::Bin, 'migrations', 'test.sql'));
is $sql2->migrations->latest, 4, 'latest version is 4';
is $sql2->migrations->active, 0, 'active version is 0';
eval { $sql2->migrations->migrate };
like $@, qr/does_not_exist/, 'right error';
is $sql2->migrations->migrate(3)->active, 3, 'active version is 3';
is $sql2->migrations->migrate(2)->active, 2, 'active version is 3';
is $sql->migrations->active, 0, 'active version is still 0';
is $sql->migrations->migrate->active, 10, 'active version is 10';
is_deeply $sql2->db->query('select * from migration_test_three')
  ->hashes->to_array, [{baz => 'just'}, {baz => 'works ♥'}],
  'right structure';
is $sql->migrations->migrate(0)->active,  0, 'active version is 0';
is $sql2->migrations->migrate(0)->active, 0, 'active version is 0';

# Unknown version
eval { $sql->migrations->migrate(23) };
like $@, qr/Version 23 has no migration/, 'right error';

done_testing();

__DATA__
@@ test1
-- 7 up
create table migration_test_four (test integer));

-- 10 up
insert into migration_test_four values (10);

@@ test2
-- 2 up
create table migration_test_five (test integer);
