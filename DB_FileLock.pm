#!/usr/local/bin/perl -w
# Tie::DB_FileLock - an implementation of a tied, locking DB_File Hash.
# Tie::FileLock.pm      6/28/1999
# John M Vinopal        banshee@resort.com
#
# Copyright (C) 1998-99, John M Vinopal, All Rights Reserved.
# This program is free software.  Permission is granted to copy
# and modify this program so long as this copyright notice is
# preserved.  This software is distributed without warranty.
# You can redistribute it and/or modify it under the same terms
# as Perl itself.
#
package Tie::DB_FileLock;
use strict;

require 5.004;
require Tie::Hash;
use Carp;
use DB_File;
use FileHandle;
use Fcntl qw(:flock O_RDONLY O_RDWR O_CREAT);

use vars qw(@ISA @EXPORT $VERSION $DEBUG);
@ISA = qw(Tie::Hash DB_File);
@EXPORT = @DB_File::EXPORT;
$VERSION = '0.08';
$DEBUG = 0;

sub TIEHASH {
	my $class = shift;
	my ($dbname, $openmode, $perms, $type) = @_;

	# Typecheck the type, only HASH and BTREE.
	if ($type and ref($type) eq "DB_File::RECNOINFO") {
		croak "Tie::DB_FileLock can only tie an array to a DB_RECNO database\n";
	}

	# Create the new hash object.
	my $self = bless {}, $class;

	# Open and Initialize the dbm.
	$self->_openDB(@_);
	# Lock the dbm for the duration.
	# XXX - Postpone lock until the first access?
	$self->lockDB if ($dbname);

	return $self;
}

# XXX - to support RECNO type
sub TIEARRAY {
	my $class = shift;
	my ($dbname, $openmode, $perms, $type) = @_;

	# Typecheck the type, only HASH and BTREE.
	if ($type and ref($type) ne "DB_File::RECNOINFO") {
		my $t = ref($type);
		$t =~ s/DB_File::(\w+)INFO/$1/;
		croak "Tie::DB_FileLock can only tie an associative array to a DB_$t database\n";
	}

	croak "DB_RECNO not implemented";
}

sub _openDB {
	my $self = shift;
	my $dbname = shift;
	my ($openmode, $perms, $type) = @_;
	my @params = @_;
	my %db;

	# Default settings.
	$openmode = O_CREAT | O_RDWR unless defined $openmode;

	# Obtain a tie to the DB Hash.
	my $dbobj = tie(%db, 'DB_File', $dbname, @params);
	croak "tie($dbname): $!" unless $dbobj;

	# Required on some OSes, else new files are not created and
	# the subsequent locking calls fail.  [Linux,Solaris,?]
	$dbobj->sync();

	# Dup a filehandle to the hash object if not in-core db.
	if ($dbname) {
		my $lockmode;
		my $fd = $dbobj->fd;
		my $fh = FileHandle->new("<&=$fd") or croak("dup: $!");
		$self->{LOCKFH} = $fh;

		# Determine type of locking.
		if ($openmode == O_RDONLY) {
			$lockmode = LOCK_SH;
		} else {
			$lockmode = LOCK_EX;
		}
		$self->{LOCKMODE} = $lockmode;
	}

	# Store object parts.
	$self->{DBNAME} = $dbname;
	$self->{TIEPARAMS} = \@params;
	$self->{OPENMODE} = $openmode;
	$self->{DBOBJ} = $dbobj;
	$self->{ORIG_DB} = \%db;
}

# Close a file.  Undef the object, untie it and undef the
# locking file handle.
sub _closeDB {
	undef $_[0]->{DBOBJ};
	untie($_[0]->{ORIG_DB}) or croak("untie: $!");
	undef($_[0]->{LOCKFH});
}

# Lock the DB, blocking until we have a lock.
sub lockDB {
	my ($self) = @_;
	my %db;

	# Block on locking the filehandle.
	flock($self->{LOCKFH}, $self->{LOCKMODE}) or croak("flock: $!");

	# Reopen the dbm to obtain the current state.
	my $dbobj = tie(%db, 'DB_File', $self->{DBNAME}, @{$self->{TIEPARAMS}});
	croak "tie($self->{DBNAME}): $!" unless $dbobj;

	# Store object parts.
	$self->{DB} = \%db;
	$self->{DBOBJ} = $dbobj;
}

# Unlock the locked DB, first sync()ing changes to disk.
sub unlockDB {
	my ($self) = @_;
	return unless $self->{LOCKMODE};
	# Sync, and release the database.
	if ($self->{LOCKMODE} == LOCK_EX) {
		$self->{DBOBJ}->sync() and croak("sync(): $!");
	}
	undef($self->{DBOBJ});
	untie($self->{DB}) or croak("untie: $!");
	undef($self->{DB});
	flock($self->{LOCKFH}, LOCK_UN) or croak("unlock: $!");
}

# Toggle debug setting and return state.
sub debug { $DEBUG = $_[1] if (@_ > 1); return $DEBUG };

# Everything unlocked and closed automatically.
sub DESTROY  { $_[0]->unlockDB(); $_[0]->_closeDB(); }

sub STORE    {
               print STDERR "STORE: @_\n" if $DEBUG;
               croak("RO hash") if $_[0]->{OPENMODE} == O_RDONLY;
               $_[0]->{DB}->{$_[1]} = $_[2];
             }
sub FETCH    { 
               print STDERR "FETCH: @_\n" if $DEBUG;
               $_[0]->{DB}->{$_[1]};
             }
sub FIRSTKEY {
               print STDERR "FIRSTKEY: @_\n" if $DEBUG;
			   # XXX - painful.  Cheaper way to reset a hash?
               my $a = scalar keys %{$_[0]->{DB}};
               each %{$_[0]->{DB}};
             }
sub NEXTKEY  {
               # NEXTKEY relies on the setup from FIRSTKEY
               print STDERR "NEXTKEY: @_\n" if $DEBUG;
               each %{$_[0]->{DB}};
             }
sub EXISTS   {
               print STDERR "EXISTS: @_\n" if $DEBUG;
               exists $_[0]->{DB}->{$_[1]};
             }
sub DELETE   {
               print STDERR "DELETE: @_\n" if $DEBUG;
               croak("RO hash") if $_[0]->{OPENMODE} == O_RDONLY;
               delete $_[0]->{DB}->{$_[1]};
             }
sub CLEAR    {
               print STDERR "CLEAR: @_\n" if $DEBUG;
               croak("RO hash") if $_[0]->{OPENMODE} == O_RDONLY;
               %{$_[0]->{DB}} = ();
             }

# XXX - use AUTOLOADER?  No RO hash warnings.
sub put { my $self = shift; $self->{DBOBJ}->put(@_); }
sub get { my $self = shift; $self->{DBOBJ}->get(@_); }
sub del { my $self = shift; $self->{DBOBJ}->del(@_); }
sub seq { my $self = shift; $self->{DBOBJ}->seq(@_); }
sub sync { my $self = shift; $self->{DBOBJ}->sync(@_); }
sub fd { my $self = shift; $self->{DBOBJ}->fd(@_); }
# XXX - BTREE only calls.
sub get_dup { my $self = shift; $self->{DBOBJ}->get_dup(@_); }
sub find_dup { my $self = shift; $self->{DBOBJ}->find_dup(@_); }
sub del_dup { my $self = shift; $self->{DBOBJ}->del_dup(@_); }
# XXX - DBM Filters
sub filter_store_key { $_[0]->{DBOBJ}->filter_store_key(@_[1..$#_]); }
sub filter_store_value { $_[0]->{DBOBJ}->filter_store_value(@_[1..$#_]); }
sub filter_fetch_key { $_[0]->{DBOBJ}->filter_fetch_key(@_[1..$#_]); }
sub filter_fetch_value { $_[0]->{DBOBJ}->filter_fetch_value(@_[1..$#_]); }

package Tie::DB_FileLock::HASHINFO;
use strict;
@Tie::DB_FileLock::HASHINFO::ISA = qw(DB_File::HASHINFO);
sub new { shift; DB_File::HASHINFO::new('DB_File::HASHINFO', @_); }

package Tie::DB_FileLock::BTREEINFO;
use strict;
@Tie::DB_FileLock::BTREEINFO::ISA = qw(DB_File::BTREEINFO);
sub new { shift; DB_File::HASHINFO::new('DB_File::BTREEINFO', @_); }

package Tie::DB_FileLock::RECNOINFO;
use strict;
@Tie::DB_FileLock::RECNOINFO::ISA = qw(DB_File::RECNOINFO);
sub new { shift; DB_File::HASHINFO::new('DB_File::RECNOINFO', @_); }

1;
__END__

=head1 NAME

Tie::DB_FileLock - Locking access to Berkeley DB 1.x

=head1 SYNOPSIS

 use Tie::DB_FileLock;

 [$X =] tie %hash, 'Tie::DB_FileLock', [$file, $flags, $mode, $DB_HASH];
 [$X =] tie %hash, 'Tie::DB_FileLock', $file, $flags, $mode, $DB_BTREE;

 $X->debug($value);

 $status = $X->del($key [, $flags]);
 $status = $X->put($key, $value [, $flags]);
 $status = $X->get($key, $value [, $flags]);
 $status = $X->seq($key, $value, $flags);
 $status = $X->sync([$flags]);
 $status = $X->fd();

 # BTREE only
 $count = $X->get_dup($key);
 @list  = $X->get_dup($key);
 %list  = $X->get_dup($key, 1);
 $status = $X->find_dup($key, $value);
 $status = $X->del_dup($key, $value);

 # DBM Filters
 $old_filter = $db->filter_store_key  ( sub { ... } );
 $old_filter = $db->filter_store_value( sub { ... } );
 $old_filter = $db->filter_fetch_key  ( sub { ... } );
 $old_filter = $db->filter_fetch_value( sub { ... } );

 untie %hash;

=head1 DESCRIPTION

Module DB_File allows perl to tie hashes to on-disk dbm files, but
fails to provide any method by which the hashes might be locked,
providing exclusive access or preventing page-level collisions.
Tie::DB_FileLock extends DB_File, providing a locking layer using
flock().

Unlike Tie::DB_Lock, Tie::DB_FileLock does not duplicate files to 
allow concurrent access for readers and writers.  Tie::DB_FileLock
is therefore suitable for large dbms with relatively short locking
periods.

Tie::DB_FileLock is designed as a drop-in replacement for DB_File,
requiring minimal code changes.  Change all occurrences of "DB_File" to
"Tie::DB_FileLock" and all should be well.  DB_RECNO is not presently
supported by Tie::DB_FileLock.

Arguments to Tie::DB_FileLock are identical as those to DB_File. 
The dbm is locked for shared access if opened RO, exclusively
otherwise.  The default, as in DB_File, is read/write/create.

Use of the predefined references $DB_HASH, $DB_BTREE, and $DB_RECNO,
is identical as with DB_File.  When creating your own, the new call is
the same, but the object created is a DB_File::XXX thing and not a
Tie::DB_FileLock::XXX thing -- therefore error messages will refer
to DB_File::XXX.

=head1 LOCKING

The locking autoline presented by 'Programming Perl' is incorrect for
multiple simultaneous writers.  The problem is that a successful flock()
lags the tie() by some amount of time.  However the snapshot of the 
on-disk dbm is that from the time of the tie() and not of the flock(),
therefore once the flock() succeeds, the dbm may have changed and 
therefore must be tie()ed again, loading the latest state of the dbm.

Locking cannot be at the level of methods like FETCH() and STORE()
because then statements like $hash{$a}++ are not atomic: that is, a
different access could (will) take place between the FETCH($a) and
the STORE($a + 1).

Therefore locking must occur at a coarser level and the programmer 
must not dawdle when locks are active.  In the case of loops,
an effort need be made to untie() the dbm periodically, permitting
other processes their due.  At some additional cost, a program may
yield access to others by breaking a loop
like:

      tie(%db, 'Tie::DB_FileLock', "arg1.db");
      foreach (1..10000) { accesses; }
      untie(%db); 

into:

      my $dbobj = tie(%db, 'Tie::DB_FileLock', "arg1.db");
      foreach (1..10000) {
        accesses;
        if ($_ % 100 == 0) {
           $dbobj->unlockDB();
           $dbobj->lockDB();
        }
      }
      untie(%db);

=head1 AUTHOR

John M Vinopal, banshee@resort.com

=head1 SEE ALSO

DB_File(3p).

=cut
