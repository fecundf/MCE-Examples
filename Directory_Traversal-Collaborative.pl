#!/user/bin/env perl

use 5.010;
use strict;
use warnings;

BEGIN {
  eval 'use threads;use threads::shared; 1'
    || eval 'use forks;use forks::shared;1'
    || die 'Need to install forks::shared or threads::shared';
}
use File::Spec::Functions 'catfile';
use Time::HiRes qw(time);
use Carp qw'verbose';
use MCE;
use MCE::Queue;
#use Benchmark qw(:hireswallclock cmpthese timethese);
BEGIN {
  # Use my local patched Benchmark, which accepts style='wallclock'
  require 'c:/Users/yhluc00/Documents/GitHub/perl/lib/Benchmark.pm';
  Benchmark->import(qw(:hireswallclock cmpthese timethese));
}


# Get the directories to start with
my @start_dirs = @ARGV
  or die "Please specify one or more starting directories, stopped";

-d or die "No such directory: $_, stopped" for @start_dirs;

# Each proces gets their own copy of $bytes, $files
my ($bytes, $files) = (0,0);
my $start = time;

my $shared_work = Shared::Queue::Simple->new;
$shared_work->enqueue($_) for @start_dirs;

my $free_count :shared = 0;

MCE->new(
  chunk_size => 1,
  max_workers => 'auto',
  input_data => sub { $shared_work->dequeue },
  user_begin => sub { $free_count = lock($free_count) + 1 },
  gather => sub { $bytes += $_[0]; $files += $_[1] },
  user_func => sub {
#    say MCE->wid," starting for $_";
    { $free_count = lock($free_count) - 1 }
    my @work = $_;

    while ($_ = pop @work, defined) {
#    say MCE->wid," popped $_";
    my @paths;
      if (ref || substr($_,0,1) eq "\x00" ) {
	# This is an array of paths, first element is directory name
	@paths = (ref) ? @$_ : split /\x00/,substr($_,1);
	my $dir = shift @paths;
#	say MCE->wid," dir=$dir";
	for my $path (@paths) {
#	  say MCE->wid," path=$path";
	  $path = catfile($dir,$path);
	  -f $path ? ( $bytes += -s _, ++$files ) :
	    push @work, $path;
	}
      } else {
	# This is a directory, expand it
	my $dir = $_;
	opendir DH,$dir or die "Cannot opendir $dir: $!";
	for (readdir DH) {
	  next if $_ eq '.' || $_ eq '..';
#    say MCE->wid," free=$free_count read $_";
	  if ($free_count && @work && -d (my $subd = catfile($dir,$_))) {
	    # Someone else is free, and we have more work to do
#    say MCE->wid," enq $dir/$_";
	    $shared_work->enqueue($subd);
	  } else {
	    push @paths,$_;
	  }
	}
	if ($free_count && @paths > 1 + lock($free_count)) {
	  # Divide up this work among the free workers
	  my $give_count = int(@paths / (1 + $free_count));
	  $shared_work->enqueue(
	    #	    shared_clone( [ $dir , splice(@paths,-$give_count) ] )
	    join( "\x00", '', $dir , splice(@paths,-$give_count) )
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
    $shared_work->enqueue(undef) if $free_count == $_[0]->max_workers;
#    say(MCE->wid." leaving with free=$free_count, max_w=".$_[0]->max_workers)
  }
)->run;



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


# mock-up a threads::shared queue
package Shared::Queue::Simple;
use threads;
use threads::shared;
use Carp qw'verbose cluck';

sub new {
  my @Q :shared;
  bless \@Q, shift @_;
}

sub enqueue {
  my $self = shift;
  lock($self);
#cluck "_enq @_";
  push @$self, @_;
  cond_broadcast($self);
}

sub dequeue {
  my ($self,$count) = @_;   # proof-of-concept ignores $count

  lock($self);
  cond_wait($self) until @$self;
  return shift @$self;
}
