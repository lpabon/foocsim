# foocsim

Simple file or object cache simulator

## Installation
In your `GOPATH` type:

```
$ go get github.com/lpabon/foocsim
$ go get github.com/lpabon/godbc
```

## Example

* Run the default simulation

```
$ cd $GOPATH/src/github.com/lpabon/foocsim/foocsim
$ go run foocsim.go
== Cache Information ==
Cache Utilization: 100.00 %
Read Hit Rate: 0.8447
Write Hit Rate: 0.8451
Read hits: 2028005
Write hits: 1350885
Delete hits: 2375962
Reads: 2400906
Writes: 1598558
Deletions: 6000536
Insertions: 1971459
Evictions: 358430
Invalidations: 1350885
```

* The simulation created a file called `cache.data`
* Run `fooplot.gp` to create graphs using `gnuplot` as follows:

```
$ ./fooplot.gp
```

### Example Plots

![deletes](images/cache_deletes.png)

![readhitrate](images/cache_readhitrate.png)

![writehitrate](images/cache_writehitrate.png)

![evictions](images/cache_evictions.png)

![reads](images/cache_reads.png)

![writes](images/cache_writes.png)

## Help

```
$ go run foocsim.go -help
Usage of foocsim:
  -cachesize=64: Cache size in GB. Default 8 GB
  -cachetype="simple": Cache type to use.  Current caches: simple, null
  -chunksize=256: Chunk size in KB. Default 256 KB
  -dataperiod=1000: Number of IOs per data collected
  -deletions=15: % of File deletions
  -ios=5000000: Number of IOs
  -maxfilesize=1048576: Maximum file size MB. Default 1 TB
  -numfiles=100000: Number of files
  -reads=65: % of Reads
  -writethrough=true: Writethrough or read miss
  -zipf_filedistribution=true: Use a Zipf or Random distribution
exit status 2
```
