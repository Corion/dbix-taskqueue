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
    max_retries => -1,
);

my @tasks= map {
    $w->queue->enqueue(
        payload => { num => $_, count => 0 },
    )
} 1..10;

my $start= time;

my %done;

$w->run(
    batch => 10,
    idle_timeout => 20, # quit after 20s of lazyness
    on_die => sub {
        # diag "$_[0] (in test)";
    },
    cb => sub {
        my( $payload, $task, $q, $quit )= @_;
        my $num= $payload->{num};
        my $count= $payload->{count};
        
        #diag sprintf "Working on task %s (Number is %d, count is %d)\n",
        #    $task->id, $num, $count;
        
        if( rand > 0.1 ) {
            if( $count < 100) {
                $q->queue->enqueue( payload => { num => $num, count => $count+1 } );
            } else {
                $done{ $num }++;
                diag "$num done";
                
                if( scalar keys %done == 10 ) {
                    $quit->();
                };
            };
            return {};
        } else {
            #diag "$num failed.";
            die "$num failed.";
        };
    },
);

is( (scalar keys %done), 10, "We processed ten tasks" );
is 0+ (grep { $_ != 1 } values %done), 0, "... and every task only once"
    #or dump_table();
    or diag Dumper \%done
    ;
