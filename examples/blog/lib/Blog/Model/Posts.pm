package Blog::Model::Posts;
use Mojo::Base -base;

has 'sqlite';

sub add {
  my ($self, $post) = @_;
  return $self->sqlite->db->insert('posts', $post)->last_insert_id;
}

sub all { shift->sqlite->db->select('posts')->hashes->to_array }

sub find {
  my ($self, $id) = @_;
  return $self->sqlite->db->select('posts', undef, {id => $id})->hash;
}

sub remove {
  my ($self, $id) = @_;
  $self->sqlite->db->delete('posts', {id => $id});
}

sub save {
  my ($self, $id, $post) = @_;
  $self->sqlite->db->update('posts', $post, {id => $id});
}

1;
