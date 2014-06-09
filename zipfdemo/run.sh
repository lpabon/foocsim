#!/bin/sh
go run zipfdemo.go && ./zipfplot.gp && firefox zipfplot.png