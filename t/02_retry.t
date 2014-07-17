#!perl -w
use strict;
use DBI;
use DBIx::TaskQueue::Worker;
use Data::Dumper;
use Test::More tests => 2;

my $w= DBIx::TaskQueue::Worker->new(
    create => 1,
    dsn => 'dbi:SQLite:dbname=:memory:',
    retry_delay => 0,
    max_retries => 3,
);

my @tasks= map {
    $w->queue->enqueue(
        payload => { num => $_, count => 0 },
    )
} 1..10;

my $start= time;

my %failed;

$w->run(
    batch => 2,
    idle_timeout => 3, # quit after 3s of lazyness
    sleep => 2,
    on_die => sub {
        $failed{ $_[0] }++;
    },
    cb => sub {
        my( $payload, $task, $q, $quit )= @_;
        my $num= $payload->{num};
        my $count= $payload->{count};
        
        # Permanent failure here
        die "$num failed.";
    },
);

is( (scalar keys %failed), 10, "We failed ten different tasks" );
is 0+ (grep { $_ != 4 } values %failed), 0, "... and every task failed once and was retried three times"
    #or dump_table();
    or diag Dumper \%failed
    ;
