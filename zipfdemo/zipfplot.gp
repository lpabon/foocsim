#!/usr/bin/env gnuplot

set terminal png
set logscale x
set output "loadrw.png"
plot "filecc.data" using 1:3 title "Reads", \
     "filecc.data" using 1:4 title "Writes"

set output "zipfplot.png"
set logscale y
plot "filecc.data" using 1:2 with impulses title "Accessed Objs"