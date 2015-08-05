#!/user/bin/env perl

use strict;
use warnings;

BEGIN {
  eval 'use threads;use threads::shared; 1'
    || eval 'use forks;use forks::shared;1'
    || die 'Need to install "forks::shared" or "threads::shared"';
}
use File::Spec::Functions 'catfile';
use Time::HiRes qw(time);
use MCE;
use MCE::Queue;


# Get the directories to start with
my @start_dirs = @ARGV
  or die "Please specify one or more starting directories, stopped";

-d or die "No such directory: $_, stopped" for @start_dirs;

my $start = time;
# Each proces gets their own copy of $bytes, $files
my ($bytes, $files) = (0,0);

# Globally shared queue, and count of how many free workers
our $shared_work = MCE::Queue->new( fast => 1, queue => \@start_dirs );
our $free_count :shared = 0;

my $mce = MCE->new(
  chunk_size => 1,
  max_workers => 'auto',
  user_begin => sub { $free_count = lock($free_count) + 1 },
  gather => sub { $bytes += $_[0]; $files += $_[1] },
  user_func => sub {
    traverse() while ($_ = $shared_work->dequeue, defined)
  }
);

my $max_workers = $mce->max_workers;
$mce->run;

sub traverse {
  { $free_count = lock($free_count) - 1 }
  my @work = $_;

  while ($_ = shift @work, defined) {
    if (ref) {
      # This is an array of paths, first element is directory name
      my $dir = shift @$_;
      for my $path (@$_) {
	# This is the work we want to do,
	# what Find::File would call its "wanted" callback.
	$path = catfile($dir,$path);
	-f $path ? ( $bytes += -s _, ++$files ) :
	  push @work, $path;
      }

    } else {

      # This is a directory, expand it
      my ($dir,@paths) = ($_);
      opendir DH,$dir or die "Internal error, cannot opendir $dir: $!";
      for (readdir DH) {
	next if $_ eq '.' || $_ eq '..';
	my $subd;
	$free_count && @work && -d ($subd = catfile($dir,$_)) ?
	  # Someone else is free, and we have more work to do
	  $shared_work->enqueue($subd)
	  :
	  # This worker will handle this path
	  push @paths,$_;
      }

      next unless @paths; # Empty directory

      if ($free_count && @paths > 1 + lock($free_count)) {
	# Divide up this work among the free workers
	my $give_count = int(@paths / (1 + $free_count));
	$shared_work->enqueue(
	  [ $dir , splice(@paths,-$give_count) ]
	) for (1 .. $free_count);
      }
      unshift @paths, $dir;
      push @work, \@paths;
    }
  }

  # Pass along our sums so far
  MCE->gather($bytes, $files) if $files;
  ($bytes, $files) = (0,0);

  # Done with our work, let everyone know we're free
  $free_count = 1 + lock($free_count);
  $free_count == $max_workers && $shared_work->enqueue((undef) x $max_workers);
}


sub show_results {
  use feature 'state';
  my($name,$bytes,$files) =@_;
  state ($check_bytes, $check_files);

  print "$name Total $bytes bytes in $files files\n";

  if (defined $check_bytes) {
    if ($bytes != $check_bytes || $files != $check_files) {
      die "Mismatch\n";
    }
  } else {
    ($check_bytes, $check_files) = ($bytes, $files)
  }
}

show_results('Collab',$bytes,$files);
printf "Duration: %0.3f seconds\n", time() - $start;
