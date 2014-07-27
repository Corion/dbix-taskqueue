package DBIx::TaskQueue::Worker;
use strict;
use 5.010; # for //
use DBIx::TaskQueue;
use Carp qw(croak carp);
use Try::Tiny;

use vars qw'$VERSION';
$VERSION = '0.01';

=head1 NAME

DBIx::TaskQueue::Worker - convenience class for implementing a worker process

=head1 SYNOPSIS

  use DBIx::TaskQueue::Worker;
  my $w= DBIx::TaskQueue::Worker->new(
      queue => 'http.fetch',
  );
  $w->run( cb => sub {
      my( $input, $task, $queue )= @_;
      # ... perform work on $input
      my $output= $input;
      return $output;
  });

=head1 METHODS

=head2 C<< DBIx::TaskQueue::Worker->new %options >>

  my $worker= DBIx::TaskQueue::Worker->new(
  )
  $worker->run();

=cut

sub new {
    my( $class, %options )= @_;
    my $queue = ref $options{ queue } ? $options{ queue } : DBIx::TaskQueue->new( %options );
    my $self= {
        queue => $queue,
    };
    bless $self => $class;
}
sub queue { $_[0]->{queue} }

=head2 C<< $queue->run %options >>

  $queue->run(
      batch => 5,
      sleep => 60, # check every minute when idle
      cb => sub {
          my( $input, $task, $queue, $quit )= @_;
      },
  );

This is a convenience routine to continously fetch tasks
and process them. This routine is basically equivalent to the
following code:

    while( my $tasks = $queue->fetch($batch) ) {
        for my $task (@$tasks) {
            $task->start
                or next; # Task was cancelled/reassigned to another worker
            try {
                my $input= $task->payload;
                
                if( not can_perform_work( $input )) {
                    $task->release;

                } else {
                    my $result= perform_work($input, $task, $queue, $quit);
                    $task->finish($result);
                };
            } catch {
                $task->fail($_);
            };
        };
    };

The C<< $quit >> callback is used to gracefully stop the loop
after this task finishes.

=cut

sub run {
    my( $self, %options )= @_;
    my $worker= $options{ cb }
        or croak "Need a callback to perform the work";
    $options{ sleep }//= 60;
    my $reserve= $options{ batch } || 1;
    
    $options{ idle_timeout }//= $self->{ idle_timeout };
    
    $options{ on_die }//= sub { carp "Worker died: $_[0]" };
    
    use Data::Dumper;
    #warn Dumper $self->queue;
    
    my $idle_since;
    
    # Throttling via Algorithm::TokenBucket / SQL
    # Adjust $reserve according to Algorithm::TokenBucket
    # Sleep until at least one token is available
    FETCH: while( my $tasks = $self->queue->fetch($reserve) ) {
        my $quit;
        TASK: for my $task (@$tasks) {
            #warn "Starting work on " . $task->id;
            $task->start
                or next; # Task was cancelled/reassigned to another worker
            try {
                my $input= $task->payload;
                
                my $do_quit= sub { $quit= 1 };
                my $result= $worker->( $input, $task, $self, $do_quit );
                $task->finish($result);
            } catch {
                $options{ on_die }->($_);
                $task->fail($_);
            };
            last FETCH if $quit;
        };
        if( ! @$tasks ) {
            # ->on_idle
            #warn "Idle";
            sleep $options{sleep};
            last FETCH
                if( $options{ idle_timeout } and time > $options{ idle_timeout } + $idle_since);
            $idle_since ||= time;
        } else {
            $idle_since= 0;
        };
    };
}

1;

=head1 SEE ALSO

L<DBIx::TaskQueue>

=cut