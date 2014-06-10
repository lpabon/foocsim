#!/usr/bin/env gnuplot

set terminal png
set datafile separator ","
set output "cache_readhitrate.png"
plot "cache.data" using 1:2 every 50 title "Read Hit Rate"

set output "cache_writehitrate.png"
plot "cache.data" using 1:3 every 50 title "Write Hit Rate"

set output "cache_reads.png"
plot "cache.data" using 1:7 every 50 title "Reads", \
	 "cache.data" using 1:4 every 50 title "Read Hits"

set output "cache_writes.png"
plot "cache.data" using 1:8 every 50 title "Writes", \
     "cache.data" using 1:5 every 50 title "Write Hits"

set output "cache_deletes.png"
plot "cache.data" using 1:9 every 50 title "Deletions", \
     "cache.data" using 1:6 every 50 title "Deletion Hits"

set output "cache_evictions.png"
plot "cache.data" using 1:11 every 50 title "Evictions"