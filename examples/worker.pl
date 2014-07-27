#!perl -w
use strict;
use DBIx::TaskQueue::Worker;

my $q= DBIx::TaskQueue::Worker->new(
    create => 1,
);

$q->run(
    cb => sub {
        my( $payload, $task )= @_;
        #warn Dumper $task;
        my $num= $payload->{num};
        
        print sprintf "Working on task %s (Number is %d)\n",
            $task->id, $num;
        
        sleep 9+rand()*3;
        if( rand > 0.5 ) {
            return {message=> "$num done."};
        } else {
            die "$num failed.";
        };
    },
);
