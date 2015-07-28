use Mojo::Base -strict;

use Test::More;

plan skip_all => 'Test::Pod::Coverage 1.04+ required for this test!'
  unless eval 'use Test::Pod::Coverage 1.04; 1';

all_pod_coverage_ok();
