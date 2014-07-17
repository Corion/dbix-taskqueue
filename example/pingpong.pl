#!perl -w
use strict;
use DBIx::TaskQueue::Worker;

my $w= DBIx::TaskQueue::Worker->new(
    create => 1,
    queue => 'pingpong',
    retry_delay => 0,
);

my @tasks= map {
    $w->queue->enqueue(
        payload => { num => $_, count => 0 },
        #start_in => (10-$_) * 5,
        start_in => 1,
        max_retries => -1,
    )
} 1..100;

my $start= time;
$w->run(
    batch => 10,
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
                $q->queue->enqueue( payload => { num => $num, count => $count }, retries => -1 );
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
