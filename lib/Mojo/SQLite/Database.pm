package Mojo::SQLite::Database;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBD::SQLite;
use Mojo::IOLoop;
use Mojo::SQLite::Results;
use Mojo::SQLite::Transaction;
use Scalar::Util 'weaken';

our $VERSION = '0.014';

our @CARP_NOT = qw(Mojo::SQLite::Migrations);

use constant DEBUG => $ENV{MOJO_PUBSUB_DEBUG} || 0;

has 'dbh';
has 'notification_poll_interval' => 0.5;
has 'sqlite';

sub new {
  my $self = shift->SUPER::new(@_);
  # Cache the last insert rowid on inserts
  if (my $dbh = $self->dbh) {
    weaken $dbh;
    $dbh->sqlite_update_hook(sub {
      $dbh->{private_mojo_last_insert_id} = $_[3] if $_[0] == DBD::SQLite::INSERT;
    });
  }
  return $self;
}

sub DESTROY {
  my $self = shift;

  # Supported on Perl 5.14+
  return if defined ${^GLOBAL_PHASE} && ${^GLOBAL_PHASE} eq 'DESTRUCT';

  return unless (my $sql = $self->sqlite) && (my $dbh = $self->dbh);
  $sql->_enqueue($dbh);
}

my %behaviors = map { ($_ => 1) } qw(deferred immediate exclusive);

sub begin {
  my $self = shift;
  if (@_) {
    my $behavior = shift;
    croak qq{Invalid transaction behavior $behavior} unless exists $behaviors{lc $behavior};
    $self->dbh->do("begin $behavior transaction");
  } else {
    $self->dbh->begin_work;
  }
  my $tx = Mojo::SQLite::Transaction->new(db => $self);
  weaken $tx->{db};
  return $tx;
}

sub disconnect {
  my $self = shift;
  $self->_unwatch;
  $self->dbh->disconnect;
}

sub is_listening { !!keys %{shift->{listen} || {}} }

sub listen {
  my ($self, $name) = @_;

  warn qq{$self listening on channel "$name"\n} if DEBUG;
  $self->{listen}{$name}++;
  $self->_init_pubsub;
  $self->_watch;

  return $self;
}

sub notify {
  my ($self, $name, $payload) = @_;

  $payload //= '';
  warn qq{$self sending notification on channel "$name": $payload\n} if DEBUG;
  $self->_init_pubsub;
  $self->query('insert into mojo_pubsub_notify (channel, payload) values (?, ?)', $name, $payload);
  $self->_notifications;

  return $self;
}

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my ($sth, $errored, $error);
  {
    local $@;
    eval {
      $sth = $self->dbh->prepare_cached($query, undef, 3);
      # If RaiseError has been disabled, we might not get a handle
      do { _bind_params($sth, @_); $sth->execute } if defined $sth;
      1;
    } or $errored = 1;
    $error = $@ if $errored;
  }

  if ($errored) {
    # Croak error for better context
    croak $error unless $cb;
    $error = $self->dbh->errstr;
  }

  # We won't have a statement handle if prepare failed in a "non-blocking"
  # query or with RaiseError disabled
  my $results = defined $sth ? Mojo::SQLite::Results->new(sth => $sth) : undef;
  $results->{last_insert_id} = $self->dbh->{private_mojo_last_insert_id} if defined $results;
  unless ($cb) {
    $self->_notifications;
    return $results;
  }

  # Still blocking, but call the callback on the next tick
  Mojo::IOLoop->next_tick(sub { $self->$cb($error, $results) });
  return $self;
}

sub unlisten {
  my ($self, $name) = @_;

  warn qq{$self is no longer listening on channel "$name"\n} if DEBUG;
  $name eq '*' ? delete $self->{listen} : delete $self->{listen}{$name};
  $self->_unwatch unless $self->is_listening;

  return $self;
}

sub _bind_params {
  my $sth = shift;
  return $sth unless @_;
  foreach my $i (0..$#_) {
    my $param = $_[$i];
    if (ref $param eq 'HASH' && exists $param->{type} && exists $param->{value}) {
      $sth->bind_param($i+1, $param->{value}, $param->{type});
    } else {
      $sth->bind_param($i+1, $param);
    }
  }
  return $sth;
}

sub _cleanup_pubsub {
  my $self = shift;
  # Delete any notifications that have been seen by all non-stale subscribers
  $self->dbh->do(q{delete from mojo_pubsub_notify where id <=
    (select min(last_notify_id) from mojo_pubsub_listen where last_updated >= strftime('%s','now','-1 days'))});
}

sub _init_pubsub {
  my $self = shift;
  return if $self->{init_pubsub} || $self->{init_pubsub}++;
  $self->sqlite->migrations->name('pubsub')->from_data->migrate;
}

sub _notifications {
  my $self = shift;
  if ($self->is_listening) {
    my $notifies = $self->dbh->selectall_arrayref('select id, channel, payload from mojo_pubsub_notify
      where id > ? order by id asc', { Slice => {} }, $self->{pubsub_last_notify_id});
    if ($notifies and @$notifies) {
      do { my $count = @$notifies; warn qq{$self has received $count notifications\n} } if DEBUG;
      $self->{pubsub_last_notify_id} = $notifies->[-1]{id};
      $self->dbh->do(q{update mojo_pubsub_listen set last_notify_id=?,
        last_updated=strftime('%s','now') where id=?}, undef,
        $self->{pubsub_last_notify_id}, $self->{pubsub_listen_id});
      $self->_cleanup_pubsub;
      foreach my $notify (@$notifies) {
        $self->emit(notification => @{$notify}{qw(channel payload)})
          if exists $self->{listen}{$notify->{channel}};
      }
    }
  }
}

sub _unwatch {
  my $self = shift;
  return unless delete $self->{watching};
  warn qq{$self is no longer watching for notifications\n} if DEBUG;
  Mojo::IOLoop->remove($self->{pubsub_timer});
  $self->emit('close') if $self->is_listening;
  local $@;
  eval { $self->query('delete from mojo_pubsub_listen where id=?', $self->{pubsub_listen_id}) };
}

sub _watch {
  my $self = shift;
  return if $self->{watching} || $self->{watching}++;
  warn qq{$self now watching for notifications\n} if DEBUG;
  Mojo::IOLoop->remove($self->{pubsub_timer}) if exists $self->{pubsub_timer};
  my $interval = $self->notification_poll_interval;
  $self->{pubsub_timer} = Mojo::IOLoop->recurring($interval => sub {
    local $@;
    $self->_unwatch if !eval { $self->_notifications; 1 }
      or !$self->is_listening;
  });
  $self->{pubsub_last_notify_id} = @{$self->query('select id from mojo_pubsub_notify
    order by id desc limit 1')->array // [0]}[0];
  $self->{pubsub_listen_id} = $self->query('insert into mojo_pubsub_listen
    (last_notify_id) values (?)', $self->{pubsub_last_notify_id})->last_insert_id;
}

1;

=head1 NAME

Mojo::SQLite::Database - Database

=head1 SYNOPSIS

  use Mojo::SQLite::Database;

  my $db = Mojo::SQLite::Database->new(sqlite => $sql, dbh => $dbh);
  $db->query('select * from foo')
    ->hashes->map(sub { $_->{bar} })->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::SQLite::Database> is a container for L<DBD::SQLite> database handles
used by L<Mojo::SQLite>.

As SQLite has no notification system, the L</"listen">, L</"notify">, and
L</"unlisten"> methods are implemented using event loop polling on the
internally managed tables C<mojo_pubsub_listen> and C<mojo_pubsub_notify>.

=head1 EVENTS

L<Mojo::SQLite::Database> inherits all events from L<Mojo::EventEmitter> and
can emit the following new ones.

=head2 close

  $db->on(close => sub {
    my $db = shift;
    ...
  });

Emitted when the database connection gets closed while waiting for
notifications.

=head2 notification

  $db->on(notification => sub {
    my ($db, $name, $payload) = @_;
    ...
  });

Emitted when a notification has been received.

=head1 ATTRIBUTES

L<Mojo::SQLite::Database> implements the following attributes.

=head2 dbh

  my $dbh = $db->dbh;
  $db     = $db->dbh(DBI->new);

L<DBD::SQLite> database handle used for all queries.

=head2 notification_poll_interval

  my $interval = $db->notification_poll_interval;
  $db          = $db->notification_poll_interval(1);

Interval in seconds to poll for notifications from L</"notify">, defaults to
C<0.5>. Note that lower values will increase pubsub responsiveness as well as
CPU utilization.

=head2 sqlite

  my $sql = $db->sqlite;
  $db     = $db->sqlite(Mojo::SQLite->new);

L<Mojo::SQLite> object this database belongs to.

=head1 METHODS

L<Mojo::SQLite::Database> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 new

  my $db = Mojo::SQLite::Database->new;
  my $db = Mojo::SQLite::Database->new(dbh => $dbh, sqlite => Mojo::SQLite->new);
  my $db = Mojo::SQLite::Database->new({dbh => $dbh, sqlite => Mojo::SQLite->new);

Construct a new L<Mojo::SQLite::Database> object.

=head2 begin

  my $tx = $db->begin;
  my $tx = $db->begin('exclusive');

Begin transaction and return L<Mojo::SQLite::Transaction> object, which will
automatically roll back the transaction unless
L<Mojo::SQLite::Transaction/"commit"> has been called before it is destroyed.

  # Insert rows in a transaction
  eval {
    my $tx = $db->begin;
    $db->query('insert into frameworks values (?)', 'Catalyst');
    $db->query('insert into frameworks values (?)', 'Mojolicious');
    $tx->commit;
  };
  say $@ if $@;

A transaction locking behavior of C<deferred>, C<immediate>, or C<exclusive>
may optionally be passed; the default in L<DBD::SQLite> is currently
C<immediate>. See L<DBD::SQLite/"Transaction and Database Locking"> for more
details.

=head2 disconnect

  $db->disconnect;

Disconnect L</"dbh"> and prevent it from getting cached again.

=head2 is_listening

  my $bool = $db->is_listening;

Check if L</"dbh"> is listening for notifications.

=head2 listen

  $db = $db->listen('foo');

Subscribe to a channel and receive L</"notification"> events when the
L<Mojo::IOLoop> event loop is running.

=head2 notify

  $db = $db->notify('foo');
  $db = $db->notify(foo => 'bar');

Notify a channel.

=head2 ping

  my $bool = $db->ping;

Check database connection.

=head2 query

  my $results = $db->query('select * from foo');
  my $results = $db->query('insert into foo values (?, ?, ?)', @values);
  my $results = $db->query('select ? as img', {type => SQL_BLOB, value => slurp 'img.jpg'});

Execute a blocking statement and return a L<Mojo::SQLite::Results> object with
the results. The L<DBD::SQLite> statement handle will be automatically reused
when it is not active anymore, to increase the performance of future queries.
Pass a hash reference containing C<type> and C<value> elements to specify the
bind type of the parameter, using types from L<DBI/"DBI Constants">; see
L<DBD::SQLite/"Blobs"> and the subsequent section for more information. You can
also append a callback for API compatibility with L<Mojo::Pg>; the query is
still executed in a blocking manner.

  $db->query('insert into foo values (?, ?, ?)' => @values => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 unlisten

  $db = $db->unlisten('foo');
  $db = $db->unlisten('*');

Unsubscribe from a channel, C<*> can be used to unsubscribe from all channels.

=head1 DEBUGGING

You can set the C<MOJO_PUBSUB_DEBUG> environment variable to get some advanced
diagnostics information printed to C<STDERR>.

  MOJO_PUBSUB_DEBUG=1

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book, C<dbook@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojo::SQLite>

=cut

__DATA__

@@ pubsub
-- 1 down
drop table mojo_pubsub_listen;
drop table mojo_pubsub_notify;

-- 1 up
drop table if exists mojo_pubsub_listen;
drop table if exists mojo_pubsub_notify;

create table mojo_pubsub_listen (
  id integer primary key autoincrement,
  last_notify_id integer not null default 0,
  last_updated integer not null default (strftime('%s','now'))
);
create index last_notify_id_idx on mojo_pubsub_listen (last_notify_id);
create index last_updated_idx on mojo_pubsub_listen (last_updated);
create table mojo_pubsub_notify (
  id integer primary key autoincrement,
  channel text not null,
  payload text not null default ''
);
create index channel_idx on mojo_pubsub_notify (channel);
