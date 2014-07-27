#!perl -w
use strict;
use DBIx::TaskQueue::Worker;

use Getopt::Long;
GetOptions(
    'start' => \my $init,
);

my $source_queue= $ARGV[0] eq 'ping' ? 'ping' : 'pong';
my $target_queue= $source_queue eq 'ping' ? 'pong' : 'ping';

my $w= DBIx::TaskQueue::Worker->new(
    create => 1,
    queue => $source_queue,
    retry_delay => 0,
);

if( $init ) {
    $w->queue->enqueue(
        payload => { num => $_, count => 0 },
        retries => -1,
    ) for 1..100;
};

my $next= DBIx::TaskQueue->new(
    create => 1,
    queue => $target_queue,
    retry_delay => 0,
);

my $start= time;
$w->run(
    batch => 10,
    sleep => 1,
    cb => sub {
        my( $payload, $task, $q, $quit )= @_;
        #warn Dumper $task;
        my $num= $payload->{num};
        my $count= $payload->{count};
        
        print sprintf "Working on task %s (Number is %d, count is %d)\n",
            $task->id, $num, $count;
        
        #sleep 9+rand()*3;
        if( rand > 0.1 ) {
            if( $count < 100) {
                $count++;
                #warn "Requeueing $num ($count)";
                $next->enqueue( payload => { num => $num, count => $count }, retries => -1 );
            } else {
                warn "$num done: $count";
            };
            #if( 60 < time - $start ) {
            #    $quit->();
            #};
            return {};
        } else {
            die "$num failed.";
        };
    },
);
