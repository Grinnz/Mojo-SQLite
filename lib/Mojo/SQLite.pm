package Mojo::SQLite;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use DBI;

our $VERSION = '0.001';

1;

=head1 NAME

Mojo::SQLite - A tiny Mojolicious wrapper for SQLite

=head1 SYNOPSIS

 use Mojo::SQLite;

=head1 DESCRIPTION

L<Mojo::SQLite> is a tiny wrapper around L<DBD::SQLite> that makes
L<SQLite|https://www.sqlite.org/> a lot of fun to use with the L<Mojolicious>
real-time web framework.

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book, C<dbook@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright 2015, Dan Book.

This library is free software; you may redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious>
