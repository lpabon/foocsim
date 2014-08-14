#!/usr/bin/env gnuplot

set terminal png
set datafile separator ","
set output "cache_readhitrate.png"
plot "cache.data" using 1:2 every 5 title "Read Hit Rate"

set output "cache_writehitrate.png"
plot "cache.data" using 1:3 every 5 title "Write Hit Rate"

set output "cache_reads.png"
plot "cache.data" using 1:7 every 5 title "Reads", \
	 "cache.data" using 1:4 every 5 title "Read Hits"

set output "cache_writes.png"
plot "cache.data" using 1:8 every 5 title "Writes", \
     "cache.data" using 1:5 every 5 title "Write Hits"

set output "cache_deletes.png"
plot "cache.data" using 1:9 every 5 title "Deletions", \
     "cache.data" using 1:6 every 5 title "Deletion Hits"

set output "cache_evictions.png"
plot "cache.data" using 1:11 every 5 title "Evictions"

set output "cache_readlatency.png"
plot "cache.data" using 1:13 every 5 title "Mean Read Latency (usecs)"

set output "cache_writelatency.png"
plot "cache.data" using 1:14 every 5 title "Mean Write Latency (usecs)"

set output "cache_deletelatency.png"
plot "cache.data" using 1:15 every 5 title "Mean Delete Latency (usecs)"
