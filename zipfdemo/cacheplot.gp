#!/usr/bin/env gnuplot

set terminal png
set datafile separator ","
set output "cache_hitrate.png"
plot "cache.data" using 1:2 every 20 title "Read Hit Rate", \
     "cache.data" using 1:3 every 20 title "Write Hit Rate"

set output "cache_rw.png"
plot "cache.data" using 1:7 every 20 title "Reads", \
	 "cache.data" using 1:4 every 20 title "Read Hits", \
     "cache.data" using 1:8 every 20 title "Writes", \
     "cache.data" using 1:5 every 20 title "Write Hits"