#!/usr/bin/env gnuplot

set terminal png
set output "zipfplot.png"
set logscale x
set logscale y
plot "filecc.data" using 1:2 with impulses
