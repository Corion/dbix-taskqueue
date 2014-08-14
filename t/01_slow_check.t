#!perl -w
use strict;
use DBI;
#use DBIx::TaskQueue::Worker;
use DBIx::TaskQueueJSON;
use Data::Dumper;
use Test::More tests => 4;

my $w= DBIx::TaskQueueJSON->new(
    create => 1,
    retry_delay => 0,
    max_retries => -1,
);

my $start= time;

my($t)= $w->enqueue(
    { number => 1, count => 0 }
);

my @got= $w->fetch();

is 0+@got, 1, "We got one task back";

$t->{payload}= { number => 1, count => 1 };
$t->start;
$t->finish({ status => 'OK'});

# Now check for all the states

for my $state (qw(pending reserved running)) {
    my @found= $w->find_tasks( status => $state );
    is 0+@found, 0, "We have no tasks lying around under '$state'";
};
