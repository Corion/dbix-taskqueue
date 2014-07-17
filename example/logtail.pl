#!perl -w
use strict;
use DBIx::TaskQueue;
use DBIx::TaskQueue::Monitor;
use DBIx::RunSQL;

my $q= DBIx::TaskQueue->new(
    create => 1,
    queue => 'pingpong',
);

my $m= DBIx::TaskQueue::Monitor->new(
    queue => $q,
);

my $last_update= 0;
my $done;
while( !$done ) {
    print sprintf "Task load: %d\n",
        $m->task_load;
    my @changed= @{ $m->changes_since($last_update) };
    $last_update= time;
    for my $task ( @changed ) {
        print $task->as_string,"\n";
    };
    
    sleep 10;
    
    $done= $m->outstanding_tasks == 0;
};

