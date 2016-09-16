#!/usr/bin/env perl
use strict;
use warnings;
use Redis::Fast;
use Mojo::Pg;
use FileHandle;
use Log::Fast;

my $db_user      = $ENV{TGCOLLECTOR_DBUSER};
my $db_password  = $ENV{TGCOLLECTOR_DBPASS} || '';
my $db_host      = $ENV{TGCOLLECTOR_DBHOST} || '';
my $db_name      = $ENV{TGCOLLECTOR_DBNAME};
my $redis_server = $ENV{TGCOLLECTOR_REDIS_SERVER} || '127.0.0.1:6379';
my $redis_db     = $ENV{TGCOLLECTOR_REDIS_DB};
my $queue        = $ENV{TGCOLLECTOR_QUEUE} || '';

my $pg_connect = sprintf 'postgresql://%s:%s@%s/%s', $db_user, $db_password, $db_host, $db_name;
my $pg         = Mojo::Pg->new($pg_connect) or die $@;
my $redis      = Redis::Fast->new(server => $redis_server) or die $@;
my $logfile    = FileHandle->new('collect.log', 'a');
my $log        = Log::Fast->new({fh=>$logfile, prefix=>'%D %T [%L] '});

$redis->select($redis_db) if $redis_db;

while(1) {
  my @sets = $redis->smembers($queue);

  if (scalar @sets) {
    foreach my $val (@sets) {
      eval { $pg->db->query('INSERT INTO logs (payload) values (?)', $val) };
      if ($@) {
        $log->ERR("error when pushing $val: " . $@);
        $logfile->flush;
      } else {
        $redis->srem($queue, $val);
      }
    }
  }

  sleep 1;
}
