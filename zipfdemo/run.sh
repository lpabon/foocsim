#!/bin/sh
go run zipfdemo.go > filecc.data && ./zipfplot.gp && firefox zipfplot.png
