package DBIx::TaskQueueJSON;
use strict;
use 5.010; # for //=
use Carp qw(croak);
use DBI;
use Sys::Hostname 'hostname';
use Algorithm::TokenBucket;
use Hash::Util qw(lock_keys);
use JSON::XS; # our serializer
use Try::Tiny; # Nicer syntax than if( not eval { ...;1 }) { ... };
use POSIX qw(strftime);
use File::Path qw(mkpath);
use File::Glob 'bsd_glob';

use vars qw'$VERSION';
$VERSION = '0.01';

=head1 NAME

DBIx::TaskQueueJSON - a persistent,low volume task queue for long running tasks

=head1 SYNOPSIS

  # Producer
  use DBIx::TaskQueueJSON;
  my $queue= DBIx::TaskQueueJSON->new(
      dsn => 'dbi:SQLite:dbname=mytaskqueue.sqlite',
      queue => 'http.fetch',
  );
  my $task= $queue->enqueue(
      payload => {
          url => 'http://example.com/',
          target => 'example.com.html',
      },
      start_in => 60, # Run that task in 10 minutes
  );
  print "Submitted task " . $task->id;

  # Consumer
  use DBIx::TaskQueueJSON::Worker;
  use LWP::Simple 'mirror';
  my $downloads= DBIx::TaskQueueJSON::Worker->new(
      dsn => 'dbi:SQLite:dbname=mytaskqueue.sqlite',
      queue => 'http.fetch',
  );
  $downloads->run(
      cb => sub {
          my( $payload )= @_;
          mirror $payload->{url} => $payload->{target}
              or die "Failed, will retry";
          return { status => 'OK', %$payload };
      },
  );

=cut

use vars qw($counter);
$counter= 0;

sub dbh { $_[0]->{dbh} };
sub new {
    my ($class,%args) = @_;
    $args{ table } ||= 'taskqueue';
    $args{ queue } ||= '';
    $args{ worker_id } //= sprintf "%s-%s", hostname, $$;
    $args{ max_running } //= 4;
    $args{ max_running_per_machine } //= 4;
    $args{ task_class } ||= 'DBIx::TaskQueue::Task';
    $args{ task_count } //= 1;
    $args{ sleep } //= 5; # We check every five seconds for new tasks
    $args{ retry_delay } //= 60;

    my $self = bless \%args, $class;

    if (delete $args{create}) {
        my $do_create= 1;
        #if( -d "$args{ table }/$args{queue}") {
        #    $do_create= 0;
        #};
        $self->create
            if $do_create;
    };
    $self;
};
sub worker_id { $_[0]->{worker_id} };
sub task_class { $_[0]->{task_class} };
sub table { $_[0]->{table} };
sub queue { $_[0]->{queue} };

sub create {
    my ($self, %options) = @_;
    
    my $table= $self->table;
    for my $state (qw(pending reserved running done failed abandoned)) {
        my $dir= $self->filespec( status => $state );
        $dir =~ s!/[^/]+$!!;
        if( ! -d $dir ) {
            mkpath $dir;
        };
    };
};

sub ts {
    my( $self, $epoch )= @_;
    $epoch //= time;
    return strftime('%Y-%m-%dT%H:%M:%S', localtime($epoch));
}

sub filespec {
    my( $self, %task_attributes )= @_;
    $task_attributes{ task_id } //= '*';
    $task_attributes{ worker_id} //= '*';
    $task_attributes{ status } //= '*';
    
    sprintf '%s/%s/%s/task.%s.%s', $self->table, $self->queue, $task_attributes{ status }, $task_attributes{ task_id }, $task_attributes{ worker_id };
}

sub unspec {
    my( $self, $filename)= @_;

                 #taskqueue/ping/pending/task.8136-1407867959-94.-
    $filename =~ m!^(.*?)/(.*?)/(.*?)/task\.([^.]+)\.([^.]+)$!
        or croak "Invalid task name '$filename'";
    {
        table => $1,
        queue => $2,
        status => $3,
        task_id => $4,
        worker_id => $5,
        _fn => $filename,
    }
}


sub rename_task {
    my( $self, $task )= @_;
    # Rename the task according to its current parameters
    croak "No old filename in task"
        unless $task->{_fn};
    my $target= $self->filespec( %$task );
    rename $task->{_fn} => $target
        or die "Couldn't move '$task->{_fn}' to '$target': $!";
    $task->{_fn}= $target;
    $task
}

sub find_tasks {
    my( $self, %attributes )= @_;
    my $fs= $self->filespec( %attributes );
    bsd_glob $fs
}

=head2 C<< ->enqueue %task >>

  my $job= $q->enqueue(
      start_in => 60, # delay job for 60 seconds
      payload => {
          url => 'http://www.example.com/',
          target => '~/downloads/www.example.com.html',
      },
  );
  say sprintf "Launched job %s",
      $job->id;

=cut

sub enqueue {
    my( $self, %task )= @_;
    delete $task{qw( worker_id started_at finished_at )};
    $task{ status }= 'pending';
    if( my $in= delete $task{ start_in }) {
        $task{ start_after }= $self->ts( time + $in );
    };
    $task{ enqueued_at }//= $self->ts();
    $task{ max_retries }//= $self->{ max_retries } // 5;
    $task{ retries }||= 0;
    $task{ queue }//= $self->{queue};
    
    $self->add_task(\%task)
}

=head2 C<< ->fetch( $max_items ) >>

    while( my $tasks= $q->fetch( 5 )) {
        for my $task ( @$tasks ) {
            ...
        }
    }

Fetches and locks up to C<$max_items> tasks
for processing by this worker. The items
are returned as an array reference, not
as a list.

This method blocks until at least one item is
available.

=cut

sub fetch {
    my( $self, $count )= @_;
    
    my $table= $self->table;

    # Try to write our worker_id into $count rows
    my $now= $self->ts;
    
    my @task_names= $self->find_tasks( status => 'pending' );
    my @found;
    while( @task_names and @found < $count) {
        my $f= shift @task_names;
        my $got_it= eval {
            $f= $self->unspec( $f );
            $f->{worker_id}= $self->worker_id;
            $f->{status}= 'reserved';
            $self->rename_task( $f );
            1
        };
        next
            if( ! $got_it );
            
        push @found,
            $self->thaw_task( $f );
    };
    if( @found ) {
        return $self->create_tasks( \@found, owner => 1 )
    } else {
        return []
    };
}

=head2 C<< ->next( $max_items ) >>

Synonym for C<< ->fetch >> to be API compatible with L<Data::Bulk::Stream>.
    
=cut

*next =\&fetch;

sub create_tasks {
    my( $self, $tasks, %options )= @_;
    $options{ task_class }||= $self->task_class;
    my @res= map {
        $_->{ _owner }= $options{ owner };
        $_->{ _queue } //= $self;
        $_->{queue}= $self->{ queue };
        #warn "New - " . ref $_->{payload};
        $options{ task_class }->new( $_ );
    } @{ $tasks };
    \@res
};

use vars qw(%serialize_column);
%serialize_column= map { $_=>1 } (qw(results payload error_info));

sub reset_worker {
    my($self, $worker_id)= @_;
    my $table= $self->table;
    $self->dbh->do(<<SQL,{},$worker_id);
        update $table
           set worker_id= null
            , status= 'pending'
        where worker_id=?
          and status in ('reserved','running')
SQL
}

sub cancel_worker {
    my($self, $worker_id)= @_;
    my $table= $self->table;
    $self->dbh->do(<<SQL,{},$worker_id);
        update $table
           set status= 'abandoned'
         where worker_id=?
           and status in ('reserved','running')
SQL
}

sub save_task {
    my($self, $task)= @_;
    my $fn= $self->filespec( %$task );

    my( $content )= $self->freeze_task( $task );

    # Create file
    my $tempname= "$fn.tmp";
    open my $fh, '>', $tempname
        or die "Couldn't create '$tempname': $!";
    # Write to file
    print { $fh } $content;
    close $fh
        or croak "Couldn't write to '$tempname': $!";
    rename $tempname => $fn
        or croak "Couldn't rename '$tempname' to '$fn': $!";
    # If we crash here, we have the same job twice
    # and no way of detecting this
    if( defined $task->{_fn} and $task->{_fn} ne $fn ) {
        #print "Removing old job '$task->{_fn}' (superceded by '$fn')\n";
        unlink $task->{_fn};
    };
    $task->{_fn}= $fn;
}

sub add_task {
    my( $self, $task )= @_;
    my $table= $self->table;
    
    $task->{enqueued_at}= $self->ts();
    
    $task->{task_id}= join "-", $$, time, $counter++; 
    $task->{worker_id}= '-';
    
    $self->save_task( $task );

    @{ $self->create_tasks([$task]) }
}

=head2 C<< $q->purge >>

  $q->purge()

Marks all reserved, pending or running tasks as abandoned.

This is mostly useful if too many bogus jobs have been submitted
to a queue and you want a fresh start.

=cut

sub purge {
    my( $self )= @_;
    my $table= $self->table;
    $self->dbh->do(<<SQL, {}, $self->{queue});
        update $table
        set status = case
            when status in ('pending','reserved') then 'cancelled'
            else 'abandoned'
        end
        where status in ('pending','running','reserved')
          and queue = ?
SQL
}

sub update_task {
    my( $self, $task )= @_;
    
    $self->save_task( $task );
    
    $task
}

sub refresh_task {
    my $self= shift;
    my $table= $self->table;
    # Change to bulk-fetch
    for my $task (@_) {
        my $item= $self->dbh->selectall_arrayref(<<SQL, { Slice => {}}, $task->{task_id}, $task->{task_id})->[0];
            SELECT *
              FROM $table
             WHERE task_id = ?
               AND run_id in (select max(run_id) from $table where task_id=?)
SQL

        @{ $task }{ keys %$item }= values %$item;
        $self->thaw_task( $task );
    };
};

sub freeze_task {
    my( $self, $task )= @_;
    my @columns= grep { !/^_/ and 'run_id' ne $_ } keys %$task;
    
    my $json= encode_json +{
        map { $_ => $task->{ $_ }} @columns
    };
    
    $json
}

sub thaw_task {
    my( $self, $task )= @_;
    my $fn= $self->filespec( %$task );
    open my $fh, '<', $fn
        or croak "Couldn't read '$fn': $!";
    #binmode $fh, ':utf8';
    
    my $json= do { local $/; <$fh> };
    %$task= (%{ decode_json( $json ) }, %$task);
    $task->{_fn}= $fn;
        
    $task
}

=head1 Dancer::Plugin::Queue API

This module also implements the L<Dancer::Plugin::Queue> API

=head2 C<< $q->add_msg >>

=head2 C<< $q->get_msg >>

=head2 C<< $q->remove_msg >>

See L<Dancer::Plugin::Queue> for more discussion.

=cut

sub add_msg {
    my( $self, $payload )= @_;
    $self->enqueue( payload => $payload );
}

sub get_msg {
    my( $self )= @_;
    my $job= $self->fetch( 1 );
    ($job, $job->payload)
}

sub remove_msg {
    my( $self, $job )= @_;
    $job->finish();
}

package DBIx::TaskQueue::Task;
use strict;

sub new {
    my( $class, $self )= @_;
    $self->{ status }||= 'pending';
    for( sort keys %DBIx::TaskQueueJSON::serialize_column) {
        $self->{ $_ }||= {};
        if( $self->{$_} and not ref $self->{$_}) {
            die "$_: Got '$self->{$_}' instead of a structure";
            #$self->{$_}= decode_json($self->{$_});
        };
    };

    bless $self => $class;
}
sub status { $_[0]->{status} };
sub queue { $_[0]->{_queue} };
sub id { $_[0]->{task_id} };
sub payload { $_[0]->{payload} };

sub retry {
    my( $self, %options ) = @_;
    
    my $delay= delete $options{ retry_delay } // $self->queue->{ retry_delay };
    $delay *= $self->{retries}; # linear backoff, for the moment
    my %copy= %$self;
    delete @copy{ qw{ run_id status worker_id }};
    $copy{ retries }++;
    $self->queue->enqueue( %copy, start_in => $delay ); # copy ourselves
};

sub DESTROY {
    $_[0]->release("Job destroyed without ->finish or ->fail")
        if $_[0]->status and $_[0]->status =~ /(reserved)/ and $_[0]->{_owner};
};

sub update {
    my( $self )= @_;
    if( my $q= $self->queue ) {
        $q->update_task( $self );
    };
}

sub start {
    my( $self )= @_;
    # XXX This should check whether we have been cancelled before running
    $self->{status}= 'running';
    $self->{started_at}= $self->queue->ts(time);
    $self->update;
    1
}

sub finish {
    my( $self, $results )= @_;
    $self->{status}= 'done';
    $self->{finished_at}= $self->queue->ts(time);
    $self->{results}= $results;
    $self->update;
}

sub release {
    my( $self, $reason )= @_;
    $self->{status}= 'pending';
    $self->{started_at}= undef;
    $self->{worker_id}= undef;
    $self->update;
}

sub cancel {
    my( $self, $error )= @_;
    $self->queue->cancel_worker( $self );
    $self->{finished_at}= $self->queue->ts(time);
    $self->{error_info}= { error => $error };
    $self->update;
}

sub fail {
    my( $self, $error )= @_;
    if( defined $self->{ retries } and ($self->{ retries } < $self->{max_retries} or $self->{ max_retries } == -1)) {
        $self->retry;
        $self->{status}= 'failed';
    } else {
        #use Data::Dumper;
        #warn "Abandoning " . Dumper($self->{payload}) . " at $self->{retries} / $self->{max_retries}";
        $self->{status}= 'abandoned';
    };
    $self->{finished_at}= $self->queue->ts(time);
    $self->{error_info}= { error => $error };
    $self->update;
}

sub active {
    my( $self )= @_;
    $self->status =~ /pending|reserved|running/
}

sub refresh_status {
    my( $self )= @_;
    $self->queue->refresh_task( $self );
}

sub as_string {
    my( $self )= @_;
    sprintf "% 8d - %s - %s - %s",
             $self->id // $self->{run_id}, $self->status, $self->{worker_id}||'', substr(JSON::XS::encode_json($self->{payload}),0,30);
    
}

1;

=head1 Task state sequence

Garish ASCII Art follows

  pending  <-------------------------------+
     |                                     |
     v                                     | ->release()
  reserved --------------+-----------------+
     |                   | ->cancel()
     |                   v
     |               cancelled
     | ->start()
     v
  running----------------+---------------------+
     | ->finish()        | ->fail()            | ->fail()
     v                   v                     v
   done                failed (retries)    abandoned (no retries)
  

=head1 Queue characteristics

=over 4

=item *

Low throughput

This queue reaches a sustained throughput of x/s messages when
using two local processes. This throughput drops to x/s messages
when using two writers and two readers. The main intent
for this queue is for long-running low-volume jobs.

=item *

Brokerless

This queue does not need a central job manager like Redis.
Just fire up the producers and consumers and be done.

=item *

Persistent

This queue is persistent.

=item *

Automatic retries

A worker will retry a job if it fails. The first
five retries will be with linear delay, after that
an exponential backoff will be used.

=item *

M:N producers / consumers

This queue supports all four distribution mechanisms,
1:1 , 1:M , M:1 and M:N. The main use cases are
1:M and M:1 .

=back

=head1 SEE ALSO

=head2 APIs

L<perlipc>

L<Data::Stream::Bulk>

L<Dancer::Plugin::Queue>

=head2 Other queue managers / queue systems

L<Minion>

L<Queue::DBI>

=head1 REPOSITORY

The public repository of this module is 
L<http://github.com/Corion/dbix-taskqueue>.

=head1 SUPPORT

The public support forum of this module is
L<http://perlmonks.org/>.

=head1 TALKS

None (yet)

=head1 BUG TRACKER

Please report bugs in this module via the RT CPAN bug queue at
L<https://rt.cpan.org/Public/Dist/Display.html?Name=DBIx-TaskQueue>
or via mail to L<dbix-taskqueue-Bugs@rt.cpan.org>.

=head1 AUTHOR

Max Maischein C<corion@cpan.org>

=head1 COPYRIGHT (c)

Copyright 2014 by Max Maischein C<corion@cpan.org>.

=head1 LICENSE

This module is released under the same terms as Perl itself.

=cut
