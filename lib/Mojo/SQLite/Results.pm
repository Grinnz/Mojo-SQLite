package Mojo::SQLite::Results;
use Mojo::Base -base;

use Mojo::Collection;
use Mojo::Util 'tablify';

our $VERSION = '0.011';

has 'sth';

sub DESTROY { shift->_decrement_refcount->_finish }

sub array { (shift->sth->fetchrow_arrayref)[0] }

sub arrays { _collect(@{shift->sth->fetchall_arrayref}) }

sub columns { shift->sth->{NAME} }

sub hash { (shift->sth->fetchrow_hashref)[0] }

sub hashes { _collect(@{shift->sth->fetchall_arrayref({})}) }

sub new { shift->SUPER::new(@_)->_increment_refcount }

sub rows { shift->sth->rows }

sub text { tablify shift->arrays }

sub _collect { Mojo::Collection->new(@_) }

sub _decrement_refcount {
  my $self = shift;
  return $self unless $self->{sth};
  my $count = $self->{sth}{private_mojo_refcount} // 0;
  $self->{sth}{private_mojo_refcount} = $count-1 if $count;
  return $self;
}

sub _finish { $_[0]{sth}->finish if $_[0]{sth} and !$_[0]{sth}{private_mojo_refcount}; $_[0] }

sub _increment_refcount {
  my $self = shift;
  return $self unless $self->{sth};
  my $count = $self->{sth}{private_mojo_refcount} // 0;
  $self->{sth}{private_mojo_refcount} = $count+1;
  return $self;
}

1;

=head1 NAME

Mojo::SQLite::Results - Results

=head1 SYNOPSIS

  use Mojo::SQLite::Results;

  my $results = Mojo::SQLite::Results->new(sth => $sth);
  $results->hashes->map(sub { $_->{foo} })->shuffle->join("\n")->say;

=head1 DESCRIPTION

L<Mojo::SQLite::Results> is a container for L<DBD::SQLite> statement handles
used by L<Mojo::SQLite::Database>.

=head1 ATTRIBUTES

L<Mojo::SQLite::Results> implements the following attributes.

=head2 sth

  my $sth  = $results->sth;
  $results = $results->sth($sth);

L<DBD::SQLite> statement handle results are fetched from.

=head1 METHODS

L<Mojo::SQLite::Results> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 new

  my $results = Mojo::SQLite::Results->new(sth => $sth);

Construct a new L<Mojo::SQLite::Results> object.

=head2 array

  my $array = $results->array;

Fetch next row from L</"sth"> and return it as an array reference.

  # Process one row at a time
  while (my $next = $results->array) {
    say $next->[3];
  }

=head2 arrays

  my $collection = $results->arrays;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing array references.

  # Process all rows at once
  say $results->arrays->reduce(sub { $a->[3] + $b->[3] });

=head2 columns

  my $columns = $results->columns;

Return column names as an array reference.

=head2 hash

  my $hash = $results->hash;

Fetch next row from L</"sth"> and return it as a hash reference.

  # Process one row at a time
  while (my $next = $results->hash) {
    say $next->{money};
  }

=head2 hashes

  my $collection = $results->hashes;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing hash references.

  # Process all rows at once
  say $results->hashes->reduce(sub { $a->{money} + $b->{money} });

=head2 rows

  my $num = $results->rows;

Number of rows. Note that for C<SELECT> statements, this count will not be
accurate until all rows have been fetched.

=head2 text

  my $text = $results->text;

Fetch all rows from L</"sth"> and turn them into a table with
L<Mojo::Util/"tablify">.

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
