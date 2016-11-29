package Mojo::SQLite::Database;
use Mojo::Base 'Mojo::EventEmitter';

use Carp qw(croak shortmess);
use DBI 'SQL_VARCHAR';
use DBD::SQLite;
use Mojo::IOLoop;
use Mojo::JSON 'to_json';
use Mojo::SQLite::Results;
use Mojo::SQLite::Transaction;
use Mojo::Util 'deprecated';
use Scalar::Util 'weaken';

our $VERSION = '1.002';

our @CARP_NOT = qw(Mojo::SQLite::Migrations);

use constant DEBUG => $ENV{MOJO_PUBSUB_DEBUG} || 0;

has [qw(dbh sqlite)];
has notification_poll_interval => sub {
  deprecated 'The notification_poll_interval attribute is deprecated and should no longer be used';
  return 0.5;
};
has results_class              => 'Mojo::SQLite::Results';

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
  return() if defined ${^GLOBAL_PHASE} && ${^GLOBAL_PHASE} eq 'DESTRUCT';

  return() unless (my $sql = $self->sqlite) && (my $dbh = $self->dbh);
  $sql->_enqueue($dbh);
}

sub begin {
  my ($self, $behavior) = @_;
  my $tx = Mojo::SQLite::Transaction->new(db => $self, behavior => $behavior);
  weaken $tx->{db};
  return $tx;
}

sub disconnect {
  my $self = shift;
  $self->_unwatch;
  $self->dbh->disconnect;
}

our $_QUERY_NOTIFICATIONS;

sub is_listening {
  deprecated 'The is_listening method is deprecated and should no longer be used' unless $_QUERY_NOTIFICATIONS;
  return !!keys %{shift->{listen} || {}};
}

sub listen {
  my ($self, $name) = @_;

  deprecated 'The listen method is deprecated and should no longer be used';

  warn qq{$self listening on channel "$name"\n} if DEBUG;
  $self->{listen}{$name}++;
  $self->_init_pubsub;
  $self->_watch;
  $self->dbh->do('insert or ignore into mojo_pubsub_listen
    (listener_id, channel) values (?, ?)', undef, $self->{listener_id}, $name);

  return $self;
}

sub notify {
  my ($self, $name, $payload) = @_;

  deprecated 'The notify method is deprecated and should no longer be used';

  $payload //= '';
  warn qq{$self sending notification on channel "$name": $payload\n} if DEBUG;
  $self->_init_pubsub;

  my $dbh = $self->dbh;
  $dbh->do('insert into mojo_pubsub_notify (channel, payload)
    values (?, ?)', undef, $name, $payload);
  my $notify_id = $dbh->{private_mojo_last_insert_id} // croak 'Failed to retrieve notify ID';
  $dbh->do('insert into mojo_pubsub_queue (listener_id, notify_id)
    select listener_id, ? from mojo_pubsub_listen where channel=?', undef, $notify_id, $name);

  $self->_notifications;

  return $self;
}

sub ping { shift->dbh->ping }

sub query {
  my ($self, $query) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $dbh = $self->dbh;

  my $prev_h = $dbh->{HandleError};
  # Better context for error messages
  local $dbh->{HandleError} = sub { $_[0] = shortmess $_[0]; ($prev_h and $prev_h->(@_)) ? 1 : 0 };

  my ($sth, $errored, $error);
  {
    local $@;
    unless (eval {
      # If RaiseError has been disabled, we might not get a handle
      if (defined($sth = $dbh->prepare_cached($query, undef, 3))) {
        _bind_params($sth, @_);
        $sth->execute;
      }
      1;
    }) { $errored = 1; $error = $@ }
  }

  die $error if $errored and !$cb; # bail out for errored "blocking" queries

  # We won't have a statement handle if prepare failed in a "non-blocking"
  # query or with RaiseError disabled
  my $results;
  if (defined $sth) {
    $results = $self->results_class->new(sth => $sth);
    $results->{last_insert_id} = $dbh->{private_mojo_last_insert_id};
  }

  unless ($cb) { # blocking
    local $_QUERY_NOTIFICATIONS = 1; # no deprecated message
    $self->_notifications;
    return $results;
  }

  # Still blocking, but call the callback on the next tick
  $error = $dbh->err ? $dbh->errstr : $errored ? ($error // 'Error running SQLite query') : undef;
  Mojo::IOLoop->next_tick(sub { $self->$cb($error, $results) });
  return $self;
}

sub tables {
  my @tables = shift->dbh->tables(undef, undef, undef, 'TABLE,VIEW,LOCAL TEMPORARY');
  my %names; # Deduplicate returned temporary table indexes
  return [grep { !$names{$_}++ } @tables];
}

sub unlisten {
  my ($self, $name) = @_;

  deprecated 'The unlisten method is deprecated and should no longer be used';

  warn qq{$self is no longer listening on channel "$name"\n} if DEBUG;
  my $dbh = $self->dbh;
  if ($name eq '*') {
    delete $self->{listen};
    $dbh->do('delete from mojo_pubsub_listen where listener_id=?', undef, $self->{listener_id});
  } else {
    delete $self->{listen}{$name};
    $dbh->do('delete from mojo_pubsub_listen where listener_id=? and channel=?', undef,
      $self->{listener_id}, $name);
  }
  $self->_unwatch unless $self->is_listening;

  return $self;
}

sub _bind_params {
  my $sth = shift;
  return $sth unless @_;
  foreach my $i (0..$#_) {
    my $param = $_[$i];
    if (ref $param eq 'HASH') {
      if (exists $param->{type} && exists $param->{value}) {
        $sth->bind_param($i+1, $param->{value}, $param->{type});
      } elsif (exists $param->{json}) {
        $sth->bind_param($i+1, to_json($param->{json}), SQL_VARCHAR);
      } else {
        croak qq{Unknown parameter hashref (no "type"/"value" or "json")};
      }
    } else {
      $sth->bind_param($i+1, $param);
    }
  }
  return $sth;
}

sub _cleanup_pubsub {
  my $self = shift;
  # Delete any stale listeners and their queues
  my $dbh = $self->dbh;
  my $listener_ids = $dbh->selectcol_arrayref(q{select id from mojo_pubsub_listener
    where last_checked < strftime('%s','now','-1 days')});
  if (@$listener_ids) {
    warn qq{$self cleaning up stale listeners @$listener_ids\n"} if DEBUG;
    my $in_str = join ',', ('?')x@$listener_ids;
    $dbh->do("delete from mojo_pubsub_queue where listener_id in ($in_str)", undef, @$listener_ids);
    $dbh->do("delete from mojo_pubsub_listen where listener_id in ($in_str)", undef, @$listener_ids);
    $dbh->do("delete from mojo_pubsub_listener where id in ($in_str)", undef, @$listener_ids);
  }
  # Delete any notifications that are no longer queued
  my $notify_ids = $dbh->selectcol_arrayref('select n.id from mojo_pubsub_notify as n
    left join mojo_pubsub_queue as q on q.notify_id=n.id where q.notify_id is null');
  if (@$notify_ids) {
    warn qq{$self cleaning up unqueued notifications @$notify_ids\n} if DEBUG;
    my $in_str = join ',', ('?')x@$notify_ids;
    $dbh->do("delete from mojo_pubsub_notify where id in ($in_str)", undef, @$notify_ids);
  }
}

sub _init_pubsub {
  my $self = shift;
  return $self if $self->{init_pubsub} || $self->{init_pubsub}++;
  $self->sqlite->migrations->name('pubsub')->from_data->migrate;
  $self->_cleanup_pubsub;
}

sub _notifications {
  my $self = shift;
  if ($self->is_listening) {
    my $dbh = $self->dbh;
    $dbh->do(q{update mojo_pubsub_listener set last_checked=strftime('%s','now')
      where id=?}, undef, $self->{listener_id});
    my $notifies = $dbh->selectall_arrayref('select n.id, n.channel, n.payload
      from mojo_pubsub_notify as n inner join mojo_pubsub_queue as q on q.notify_id=n.id
      where q.listener_id=? order by n.id asc', { Slice => {} }, $self->{listener_id});
    if ($notifies and @$notifies) {
      do { my @ids = map { $_->{id} } @$notifies; warn qq{$self has received notifications @ids\n} } if DEBUG;
      my $in_str = join ',', ('?')x@$notifies;
      $dbh->do("delete from mojo_pubsub_queue where listener_id=? and notify_id in ($in_str)", undef,
        $self->{listener_id}, map { $_->{id} } @$notifies);
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
  return $self unless delete $self->{watching};
  warn qq{$self is no longer watching for notifications\n} if DEBUG;
  Mojo::IOLoop->remove($self->{pubsub_timer});
  my $pid = delete $self->{listener_pid};
  if ($pid and $pid eq $$) {
    local $@;
    eval { $self->dbh->do('delete from mojo_pubsub_listener where id=?', undef, delete $self->{listener_id}) };
  }
  $self->emit('close') if $self->is_listening;
}

sub _watch {
  my $self = shift;
  return $self if $self->{watching} || $self->{watching}++;
  warn qq{$self now watching for notifications\n} if DEBUG;
  Mojo::IOLoop->remove($self->{pubsub_timer}) if exists $self->{pubsub_timer};
  my $interval = $self->notification_poll_interval;
  $self->{pubsub_timer} = Mojo::IOLoop->recurring($interval => sub {
    local $@;
    $self->_unwatch if !eval { $self->_notifications; 1 }
      or !$self->is_listening;
  });
  my $dbh = $self->dbh;
  $dbh->do('insert into mojo_pubsub_listener default values');
  $self->{listener_id} = $dbh->{private_mojo_last_insert_id} // die 'Unable to retrieve listener ID';
  $self->{listener_pid} = $$;
}

1;

=encoding utf8

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
  $db     = $db->dbh($dbh);

L<DBD::SQLite> database handle used for all queries.

  # Use DBI utility methods
  my $quoted = $db->dbh->quote_identifier('foo.bar');

=head2 notification_poll_interval

This attribute is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

=head2 results_class

  my $class = $db->results_class;
  $db       = $db->results_class('MyApp::Results');

Class to be used by L</"query">, defaults to L<Mojo::SQLite::Results>. Note
that this class needs to have already been loaded before L</"query"> is called.

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

Disconnect L</"dbh"> and prevent it from getting reused.

=head2 is_listening

This method is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

=head2 listen

This method is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

=head2 notify

This method is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

=head2 ping

  my $bool = $db->ping;

Check database connection.

=head2 query

  my $results = $db->query('select * from foo');
  my $results = $db->query('insert into foo values (?, ?, ?)', @values);
  my $results = $db->query('select ? as img', {type => SQL_BLOB, value => slurp 'img.jpg'});
  my $results = $db->query('select ? as foo', {json => {bar => 'baz'}});

Execute a blocking L<SQL|http://www.postgresql.org/docs/current/static/sql.html>
statement and return a results object based on L</"results_class"> with the
query results. The L<DBD::SQLite> statement handle will be automatically reused
when it is not active anymore, to increase the performance of future queries.
You can also append a callback for API compatibility with L<Mojo::Pg>; the
query is still executed in a blocking manner.

  $db->query('insert into foo values (?, ?, ?)' => @values => sub {
    my ($db, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

Hash reference arguments containing C<type> and C<value> elements will use the
specified bind type for the parameter, using types from L<DBI/"DBI Constants">;
see L<DBD::SQLite/"Blobs"> and the subsequent section for more information.

Hash reference arguments containing a value named C<json> will be encoded to
L<JSON text|http://sqlite.org/json1.html> with L<Mojo::JSON/"to_json">. To
accomplish the reverse, you can use the method L<Mojo::SQLite::Results/"expand">
to decode JSON text fields to Perl values with L<Mojo::JSON/"from_json">.

  # "I ♥ SQLite!"
  $db->query('select ? as foo', {json => {bar => 'I ♥ SQLite!'}})
    ->expand(json => 'foo')->hash->{foo}{bar};

=head2 tables

  my $tables = $db->tables;

Return table and view names for this database, that are visible to the current
user and not internal, as an array reference. Names will be quoted and prefixed
by a schema name of C<"main"> for standard tables, C<"temp"> for temporary
tables, and the appropriate schema name for
L<attached databases|http://sqlite.org/lang_attach.html>.

  # Names of all tables
  say for @{$db->tables};

=head2 unlisten

This method is L<DEPRECATED|Mojo::SQLite::PubSub/"DESCRIPTION">.

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
drop table mojo_pubsub_listener;
drop table mojo_pubsub_listen;
drop table mojo_pubsub_notify;
drop table mojo_pubsub_queue;

-- 1 up
drop table if exists mojo_pubsub_listener;
drop table if exists mojo_pubsub_listen;
drop table if exists mojo_pubsub_notify;
drop table if exists mojo_pubsub_queue;

create table mojo_pubsub_listener (
  id integer primary key autoincrement,
  last_checked integer not null default (strftime('%s','now'))
);
create index mojo_listener_last_checked_idx on mojo_pubsub_listener (last_checked);

create table mojo_pubsub_listen (
  listener_id integer not null,
  channel text not null,
  primary key (listener_id, channel)
);
create index mojo_listen_channel_idx on mojo_pubsub_listen (channel);

create table mojo_pubsub_notify (
  id integer primary key autoincrement,
  channel text not null,
  payload text not null default ''
);
create index mojo_notify_channel_idx on mojo_pubsub_notify (channel);

create table mojo_pubsub_queue (
  listener_id integer not null,
  notify_id integer not null,
  primary key (listener_id, notify_id)
);
create index mojo_queue_notify_id_idx on mojo_pubsub_queue (notify_id);
