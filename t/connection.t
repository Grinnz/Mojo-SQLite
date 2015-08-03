use Mojo::Base -strict;

use Test::More;
use Mojo::SQLite;
use URI::file;
use URI::QueryParam;

# Defaults
my $sql = Mojo::SQLite->new;
like $sql->dsn,    qr/^dbi:SQLite:uri=file:/, 'right data source';
is $sql->username, undef,                     'no username';
is $sql->password, undef,                     'no password';
my $options = {
  AutoCommit          => 1,
  AutoInactiveDestroy => 1,
  PrintError          => 0,
  RaiseError          => 1,
  sqlite_unicode      => 1,
};
is_deeply $sql->options, $options, 'right options';

# Minimal connection string with file
$sql = Mojo::SQLite->new('test.db');
is $sql->dsn, 'dbi:SQLite:uri=file:test.db', 'right data source';
$options = {
  AutoCommit          => 1,
  AutoInactiveDestroy => 1,
  PrintError          => 0,
  RaiseError          => 1,
  sqlite_unicode      => 1,
};
is_deeply $sql->options, $options, 'right options';

# Minimal connection string with in-memory database and option
$sql = Mojo::SQLite->new(':memory:?PrintError=1');
is $sql->dsn, 'dbi:SQLite:uri=file::memory:', 'right data source';
$options = {
  AutoCommit          => 1,
  AutoInactiveDestroy => 1,
  PrintError          => 1,
  RaiseError          => 1,
  sqlite_unicode      => 1,
};
is_deeply $sql->options, $options, 'right options';

# Connection string with absolute filename and options
my $uri = URI::file->new('/tmp/sqlite.db?#', 'unix');
$uri->query_form_hash({PrintError => 1, RaiseError => 0});
$sql = Mojo::SQLite->new($uri);
is $sql->dsn, 'dbi:SQLite:uri=file:/tmp/sqlite.db%3F%23', 'right data source';
$options = {
  AutoCommit          => 1,
  AutoInactiveDestroy => 1,
  PrintError          => 1,
  RaiseError          => 0,
  sqlite_unicode      => 1,
};
is_deeply $sql->options, $options, 'right options';

# Connection string with lots of zeros
$sql = Mojo::SQLite->new('0?RaiseError=0');
is $sql->dsn, 'dbi:SQLite:uri=file:0', 'right data source';
$options = {
  AutoCommit          => 1,
  AutoInactiveDestroy => 1,
  PrintError          => 0,
  RaiseError          => 0,
  sqlite_unicode      => 1,
};
is_deeply $sql->options, $options, 'right options';

# Invalid connection string
eval { Mojo::SQLite->new('http://localhost:3000/test') };
like $@, qr/Invalid SQLite connection string/, 'right error';

done_testing();
