#!perl -w
use strict;
use DBIx::TaskQueue;

my $q= DBIx::TaskQueue->new(
    create => 1,
);

my @tasks= map {
    $q->enqueue(
        payload => { num => $_ },
        start_in => (10-$_) * 5,
        retries => 5,
    )
} 1..10;

my $last_update= 0;
my $done;
while( !$done ) {
    print sprintf "Task load: %d\n",
        $q->task_load;
    my @changed= @{ $q->changes_since($last_update) };
    $last_update= time;
    for my $task ( @changed ) {
        print $task->as_string,"\n";
    };
    
    sleep 10;
    
    $done= 1;
    $q->refresh_task( @tasks );
    for( @tasks ) {
        if( $_->active ) {
            $done= 0;
        };
    };
};