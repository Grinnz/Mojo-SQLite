package Mojo::SQLite::PubSub;
use Mojo::Base 'Mojo::EventEmitter';

use Scalar::Util 'weaken';

our $VERSION = '0.015';

has 'poll_interval';
has 'sqlite';

sub listen {
  my ($self, $name, $cb) = @_;
  $self->_db->listen($name) unless @{$self->{chans}{$name} ||= []};
  push @{$self->{chans}{$name}}, $cb;
  return $cb;
}

sub notify { $_[0]->_db->notify(@_[1, 2]) and return $_[0] }

sub unlisten {
  my ($self, $name, $cb) = @_;
  my $chan = $self->{chans}{$name};
  @$chan = grep { $cb ne $_ } @$chan;
  $self->_db->unlisten($name) and delete $self->{chans}{$name} unless @$chan;
  return $self;
}

sub _db {
  my $self = shift;

  # Fork-safety
  delete @$self{qw(chans pid)} and $self->{db} and $self->{db}->disconnect
    unless ($self->{pid} //= $$) eq $$;

  return $self->{db} if $self->{db};

  my $db = $self->{db} =
    $self->sqlite->db(notification_poll_interval => $self->poll_interval);
  weaken $db->{sqlite};
  weaken $self;
  $db->on(
    notification => sub {
      my ($db, $name, $payload) = @_;
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
  $db->listen($_) for keys %{$self->{chans}}, 'mojo_sqlite_pubsub';
  $self->emit(reconnect => $db);

  return $db;
}

1;

=head1 NAME

Mojo::SQLite::PubSub - Publish/Subscribe

=head1 SYNOPSIS

  use Mojo::SQLite::PubSub;

  my $pubsub = Mojo::SQLite::PubSub->new(sqlite => $sql);
  my $cb = $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "Received: $payload";
  });
  $pubsub->notify(foo => 'bar');
  $pubsub->unlisten(foo => $cb);

=head1 DESCRIPTION

L<Mojo::SQLite::PubSub> is a scalable implementation of the publish/subscribe
pattern used by L<Mojo::SQLite>. It allows many consumers to share the same
database connection, to avoid many common scalability problems. As SQLite has
no notification system, it is implemented via event loop polling in
L<Mojo::SQLite::Database>, using automatically created tables prefixed with
C<mojo_pubsub>.

=head1 EVENTS

L<Mojo::SQLite::PubSub> inherits all events from L<Mojo::EventEmitter> and can
emit the following new ones.

=head2 reconnect

  $pubsub->on(reconnect => sub {
    my ($pubsub, $db) = @_;
    ...
  });

Emitted after switching to a new database connection for sending and receiving
notifications.

=head1 ATTRIBUTES

L<Mojo::SQLite::PubSub> implements the following attributes.

=head2 poll_interval

  my $interval = $pubsub->poll_interval;
  $pubsub      = $pubsub->poll_interval(0.25);

Interval in seconds to poll for notifications from L</"notify">, passed along
to L<Mojo::SQLite::Database/"notification_poll_interval">. Note that lower
values will increase pubsub responsiveness as well as CPU utilization.

=head2 sqlite

  my $sql = $pubsub->sqlite;
  $pubsub = $pubsub->sqlite(Mojo::SQLite->new);

L<Mojo::SQLite> object this publish/subscribe container belongs to.

=head1 METHODS

L<Mojo::SQLite::PubSub> inherits all methods from L<Mojo::EventEmitter> and
implements the following new ones.

=head2 listen

  my $cb = $pubsub->listen(foo => sub {...});

Subscribe to a channel, there is no limit on how many subscribers a channel can
have.

  # Subscribe to the same channel twice
  $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "One: $payload";
  });
  $pubsub->listen(foo => sub {
    my ($pubsub, $payload) = @_;
    say "Two: $payload";
  });

=head2 notify

  $pubsub = $pubsub->notify('foo');
  $pubsub = $pubsub->notify(foo => 'bar');

Notify a channel.

=head2 unlisten

  $pubsub = $pubsub->unlisten(foo => $cb);

Unsubscribe from a channel.

=head1 SEE ALSO

L<Mojo::SQLite>, L<Mojo::SQLite::Database>
