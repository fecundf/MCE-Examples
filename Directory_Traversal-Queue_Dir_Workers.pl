use strict;
use warnings;

## usage: perl files_flow.pl [ startdir ]

use MCE::Flow;
use MCE::Queue;
use Time::HiRes qw(sleep time);

my $D = MCE::Queue->new( queue => [ $ARGV[0] || '.' ] );
my $F = MCE::Queue->new( fast => 1 );

my $providers = 3;
my $consumers = 8;

MCE::Flow::init {
   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $F->enqueue((undef) x $consumers)
         if $task_name eq 'dir';
   }
};

## Notice max_workers and task_name taking
## an anonymous array to configure both tasks.

my ($bytes, $files, $start) = (0,0,time);

mce_flow {
   gather      => sub { $bytes += $_[0]; $files += $_[1] },
   max_workers => [ $providers, $consumers ],
   task_name   => [ 'dir', 'file' ],
},
sub {
   ## Dir Task. Allow time for wid 1 to enqueue dir entries.
   ## Otherwise, workers (wid 2+) may terminate early.
   sleep 0.1 if MCE->task_wid > 1;

   while (defined (my $dir = $D->dequeue_nb)) {
       my (@files, @dirs);
       opendir my($DH),$dir;
       foreach (grep !/^\.\.?$/, readdir $DH) {
	 my $path = "$dir/$_";
         if (-d $path) { push @dirs, $path; }
         else { push @files, $path; }
      }
      $D->enqueue(@dirs ) if scalar @dirs;
      $F->enqueue(@files) if scalar @files;
   }
},
sub {
   ## File Task.
   local $SIG{__WARN__} = sub {};
   my ($bytes, $files) = (0,0);
   while (defined (my $file = $F->dequeue)) {
      $bytes += -s $file;
      $files++;
   }
   MCE->gather($bytes, $files);
};

MCE::Flow::finish;

print  "Total $bytes bytes in $files files\n";
printf "Duration: %0.3f seconds\n", time() - $start;
