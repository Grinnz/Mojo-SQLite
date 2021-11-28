package Mojo::SQLite::Transaction;
use Mojo::Base -base;

use Carp 'croak';

our $VERSION = '3.009';

has db => undef, weak => 1;

my %behaviors = map { ($_ => 1) } qw(deferred immediate exclusive);

sub new {
  my $self = shift->SUPER::new(@_, rollback => 1);
  my $dbh = $self->{dbh} = $self->db->dbh;
  if (my $behavior = $self->{behavior}) {
    croak qq{Invalid transaction behavior $behavior} unless exists $behaviors{lc $behavior};
    $dbh->do("begin $behavior transaction");
  } else {
    $dbh->begin_work;
  }
  return $self;
}

sub DESTROY {
  my $self = shift;
  if ($self->{rollback} && (my $dbh = $self->{dbh})) { $dbh->rollback }
}

sub commit {
  my $self = shift;
  $self->{dbh}->commit if delete $self->{rollback};
}

1;

=head1 NAME

Mojo::SQLite::Transaction - Transaction

=head1 SYNOPSIS

  use Mojo::SQLite::Transaction;

  my $tx = Mojo::SQLite::Transaction->new(db => $db);
  $tx->commit;

=head1 DESCRIPTION

L<Mojo::SQLite::Transaction> is a scope guard for L<DBD::SQLite> transactions
used by L<Mojo::SQLite::Database>.

=head1 ATTRIBUTES

L<Mojo::SQLite::Transaction> implements the following attributes.

=head2 db

  my $db = $tx->db;
  $tx    = $tx->db(Mojo::SQLite::Database->new);

L<Mojo::SQLite::Database> object this transaction belongs to. Note that this
attribute is weakened.

=head1 METHODS

L<Mojo::SQLite::Transaction> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 new

  my $tx = Mojo::SQLite::Transaction->new;
  my $tx = Mojo::SQLite::Transaction->new(db => Mojo::SQLite::Database->new);
  my $tx = Mojo::SQLite::Transaction->new({db => Mojo::SQLite::Database->new});

Construct a new L<Mojo::SQLite::Transaction> object.

=head2 commit

  $tx->commit;

Commit transaction.

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
