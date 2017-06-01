package Mojo::SQLite::PubSub;
use Mojo::Base 'Mojo::EventEmitter';

use Mojo::JSON qw(from_json to_json);
use Mojo::Util 'deprecated';
use Scalar::Util 'weaken';

our $VERSION = '2.003';

has [qw(poll_interval sqlite)];

sub new {
  my $class = shift;
  deprecated 'Mojo::SQLite::PubSub is deprecated and should no longer be used';
  return $class->SUPER::new(@_);
}

sub DESTROY { shift->_cleanup }

sub json { ++$_[0]{json}{$_[1]} and return $_[0] }

sub listen {
  my ($self, $name, $cb) = @_;
  $self->_db->listen($name) unless @{$self->{chans}{$name} ||= []};
  push @{$self->{chans}{$name}}, $cb;
  return $cb;
}

sub notify { $_[0]->_db->notify(_json(@_)) and return $_[0] }

sub unlisten {
  my ($self, $name, $cb) = @_;
  my $chan = $self->{chans}{$name};
  @$chan = $cb ? grep { $cb ne $_ } @$chan : ();
  $self->_db->unlisten($name) and delete $self->{chans}{$name} unless @$chan;
  return $self;
}

sub _cleanup {
  my $self = shift;
  $self->{db}->_unwatch;
  delete @$self{qw(chans db pid)};
}

sub _db {
  my $self = shift;

  # Fork-safety
  $self->_cleanup unless ($self->{pid} //= $$) eq $$;

  return $self->{db} if $self->{db};

  my $db = $self->{db} = $self->sqlite->db;
  $db->notification_poll_interval($self->poll_interval) if defined $self->poll_interval;
  weaken $db->{sqlite};
  weaken $self;
  $db->on(
    notification => sub {
      my ($db, $name, $payload) = @_;
      $payload = eval { from_json $payload } if $self->{json}{$name};
      for my $cb (@{$self->{chans}{$name}}) { $self->$cb($payload) }
    }
  );
  $db->once(
    close => sub {
      local $@;
      delete $self->{db};
      eval { $self->_db };
    }
  );
  $db->listen($_) for keys %{$self->{chans}}, 'mojo.pubsub';
  $self->emit(reconnect => $db);

  return $db;
}

sub _json { $_[1], $_[0]{json}{$_[1]} ? to_json $_[2] : $_[2] }

1;

=encoding utf8

=head1 NAME

Mojo::SQLite::PubSub - (DEPRECATED) Publish/Subscribe

=head1 DESCRIPTION

L<Mojo::SQLite::PubSub> is DEPRECATED. It was originally written as a toy
following the API of L<Mojo::Pg::PubSub>, but as SQLite is serverless and has
no ability to notify clients, it is not possible to implement an efficient
pubsub system as in for example PostgreSQL, Redis, or websockets. Consider
instead using the pubsub facilities of L<Mojo::Pg>, L<Mojo::Redis2>, or
L<Mercury|mercury>.

=head1 SEE ALSO

L<Mojo::Pg::PubSub>, L<Mojo::Redis2>, L<mercury>

=for Pod::Coverage *EVERYTHING*
