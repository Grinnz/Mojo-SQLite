package Mojo::SQLite::Results;
use Mojo::Base -base;

use Mojo::Collection;
use Mojo::JSON 'from_json';
use Mojo::Util 'tablify';

our $VERSION = '3.009';

has [qw(db sth)];

sub new {
  my $self = shift->SUPER::new(@_);
  ($self->{sth}{private_mojo_refcount} //= 0)++;
  return $self;
}

sub DESTROY {
  my $self = shift;
  return() unless my $sth = $self->{sth};
  $sth->finish unless --$sth->{private_mojo_refcount};
}

sub array { ($_[0]->_expand($_[0]->sth->fetchrow_arrayref))[0] }

sub arrays { _collect($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref})) }

sub columns { shift->sth->{NAME} }

sub expand {
  my ($self, %expands) = @_;
  for my $type (keys %expands) {
    my @cols = ref $expands{$type} eq 'ARRAY' ? @{$expands{$type}} : $expands{$type};
    ++$self->{expand}{$type}{$_} for @cols;
  }
  return $self;
}

sub finish { shift->sth->finish }

sub hash { ($_[0]->_expand($_[0]->sth->fetchrow_hashref))[0] }

sub hashes { _collect($_[0]->_expand(@{$_[0]->sth->fetchall_arrayref({})})) }

sub last_insert_id { shift->{last_insert_id} // 0 }

sub rows { shift->sth->rows }

sub text { tablify shift->arrays }

sub _collect { Mojo::Collection->new(@_) }

sub _expand {
  my ($self, @rows) = @_;
  
  return @rows unless $self->{expand} and $rows[0];
  
  if (ref $rows[0] eq 'HASH') {
    my @json_names = keys %{$self->{expand}{json}};
    for my $r (@rows) { $r->{$_} = from_json $r->{$_} for grep { $r->{$_} } @json_names }
  } else {
    my $cols = $self->columns;
    my @json_idxs = grep { $self->{expand}{json}{$cols->[$_]} } 0..$#$cols;
    for my $r (@rows) { $r->[$_] = from_json $r->[$_] for grep { $r->[$_] } @json_idxs }
  }
  
  return @rows;
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

=head2 db

  my $db   = $results->db;
  $results = $results->db(Mojo::SQLite::Database->new);

L<Mojo::SQLite::Database> object these results belong to.

=head2 sth

  my $sth  = $results->sth;
  $results = $results->sth($sth);

L<DBD::SQLite> statement handle results are fetched from.

=head1 METHODS

L<Mojo::SQLite::Results> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 new

  my $results = Mojo::SQLite::Results->new;
  my $results = Mojo::SQLite::Results->new(sth => $sth);
  my $results = Mojo::SQLite::Results->new({sth => $sth});

Construct a new L<Mojo::SQLite::Results> object.

=head2 array

  my $array = $results->array;

Fetch next row from L</"sth"> and return it as an array reference. Note that
L</"finish"> needs to be called if you are not fetching all the possible rows.

  # Process one row at a time
  while (my $next = $results->array) {
    say $next->[3];
  }

=head2 arrays

  my $collection = $results->arrays;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing array references.

  # Process all rows at once
  say $results->arrays->reduce(sub { $a + $b->[3] }, 0);

=head2 columns

  my $columns = $results->columns;

Return column names as an array reference.

  # Names of all columns
  say for @{$results->columns};

=head2 expand

  $results = $results->expand(json => 'some_json');
  $results = $results->expand(json => ['some_json','other_json']);

Decode specified fields from a particular format to Perl values for all rows.
Currently only the C<json> text format is recognized. The names must exactly
match the column names as returned by L</"columns">; it is recommended to use
explicit aliases in the query for consistent column names.

  # Expand JSON
  $results->expand(json => 'json_field')->hashes->map(sub { $_->{foo}{bar} })->join("\n")->say;

=head2 finish

  $results->finish;

Indicate that you are finished with L</"sth"> and will not be fetching all the
remaining rows.

=head2 hash

  my $hash = $results->hash;

Fetch next row from L</"sth"> and return it as a hash reference. Note that
L</"finish"> needs to be called if you are not fetching all the possible rows.

  # Process one row at a time
  while (my $next = $results->hash) {
    say $next->{money};
  }

=head2 hashes

  my $collection = $results->hashes;

Fetch all rows from L</"sth"> and return them as a L<Mojo::Collection> object
containing hash references.

  # Process all rows at once
  say $results->hashes->reduce(sub { $a + $b->{money} }, 0);

=head2 last_insert_id

  my $id = $results->last_insert_id;

Returns the L<rowid|https://www.sqlite.org/c3ref/last_insert_rowid.html> of the
most recent successful C<INSERT>.

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
