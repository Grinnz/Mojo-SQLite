-- 1 up
create table if not exists posts (
  id    integer primary key autoincrement,
  title text,
  body  text
);

-- 1 down
drop table if exists posts;
