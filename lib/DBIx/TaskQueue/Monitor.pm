package DBIx::TaskQueue::Monitor;
use strict;
use Carp qw(croak);

sub new {
    my( $class, %options );
    
    if( 2 == @_ ) {
        $class= shift;
        $options{ queue }= shift;
    } else {
        ($class,%options) = @_;
    };
    
    croak "Need a queue"
        unless $options{ queue };
    
    bless \%options => $class;
}

sub dbh { $_[0]->{queue}->dbh }
sub table { $_[0]->{queue}->table }
sub queue { $_[0]->{queue} }
sub ts { $_[0]->{queue}->ts($_[1]) }
sub create_tasks { shift->{queue}->create_tasks(@_) }

sub running_tasks {
    my ($self) = @_;
    my $running_tasks = $self->dbh->selectall_arrayref(<<SQL);
        SELECT count(*)
        FROM queue
        WHERE worker_id IS NOT NULL
          AND status='running'
          AND queue=?
SQL

    $running_tasks->[0]->[0]
};

sub pending_tasks {
    my ($self) = @_;
    my $table= $self->table;
    my $pending_tasks = $self->dbh->selectall_arrayref(<<SQL);
        SELECT count(*)
        FROM $table
        WHERE status IN ('pending', 'reserved')
SQL

    $pending_tasks->[0]->[0]
};

sub outstanding_tasks {
    my ($self) = @_;
    my $table= $self->table;
    my $pending_tasks = $self->dbh->selectall_arrayref(<<SQL);
        SELECT count(*)
        FROM $table
        WHERE status IN ('pending', 'reserved','running')
SQL

    $pending_tasks->[0]->[0]
};

sub task_load {
    my ($self) = @_;
    my $table= $self->table;
    my $now= $self->ts();
    my $count = $self->dbh->selectall_arrayref(<<SQL, {}, $now);
        SELECT count(*)
         FROM $table
        WHERE status IN ('pending', 'reserved', 'running')
          and start_after <= ?
SQL
    $count->[0]->[0]
};

sub changes_since {
    my ($self, $since) = @_;
    $since ||= time-60;
    if( $since =~ /^\d+$/) {
        # Convert epoch to timestamp
        $since= $self->ts($since);
    };
    my $table= $self->table;
    my $changed_tasks = $self->dbh->selectall_arrayref(<<SQL, { Slice => {}}, $since, $since, $since);
        SELECT *
        FROM $table
        WHERE
              enqueued_at > ?
           or started_at > ?
           or finished_at > ?
SQL

    $self->queue->thaw_task( $_ )
        for @$changed_tasks;

    return
        $self->create_tasks( $changed_tasks );
};

1;