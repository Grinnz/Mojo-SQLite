package Blog::Model::Posts;
use Mojo::Base -base;

has 'sqlite';

sub add {
  my ($self, $post) = @_;
  my $db = $self->sqlite->db;
  my $sql = 'insert into posts (title, body) values (?, ?)';
  $db->query($sql, $post->{title}, $post->{body});
  return $db->query('select last_insert_rowid()')->array->[0];
}

sub all { shift->sqlite->db->query('select * from posts')->hashes->to_array }

sub find {
  my ($self, $id) = @_;
  return $self->sqlite->db->query('select * from posts where id = ?', $id)->hash;
}

sub remove { shift->sqlite->db->query('delete from posts where id = ?', shift) }

sub save {
  my ($self, $id, $post) = @_;
  my $sql = 'update posts set title = ?, body = ? where id = ?';
  $self->sqlite->db->query($sql, $post->{title}, $post->{body}, $id);
}

1;
