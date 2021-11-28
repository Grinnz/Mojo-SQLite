package Mojo::SQLite::PubSub;
use Mojo::Base -strict;

use Mojo::Util 'deprecated';

our $VERSION = '3.009';

deprecated 'Mojo::SQLite::PubSub is deprecated and should no longer be used';

1;

=encoding utf8

=head1 NAME

Mojo::SQLite::PubSub - (DEPRECATED) Publish/Subscribe

=head1 DESCRIPTION

L<Mojo::SQLite::PubSub> is DEPRECATED and now an empty package. It was
originally written as a toy following the API of L<Mojo::Pg::PubSub>, but as
SQLite is serverless and has no ability to notify clients, it is not possible
to implement an efficient pubsub system as in for example PostgreSQL, Redis, or
websockets. Consider instead using the pubsub facilities of L<Mojo::Pg>,
L<Mojo::Redis2>, or L<Mercury|mercury>.

=head1 SEE ALSO

L<Mojo::Pg::PubSub>, L<Mojo::Redis2>, L<mercury>

=for Pod::Coverage *EVERYTHING*
