package caches

type Caches interface {
	Invalidate(chunkkey string)
	Evict()
	Insert(chunkkey string)
	Write(obj string, chunk string)
	Read(obj, chunk string)
	Delete(obj string)
	ReadHitRate() float64
	WriteHitRate() float64
	ReadHitRateDelta(prev *Caches) float64
	WriteHitRateDelta(prev *Caches) float64
	String() string
	Dump() string
	DumpDelta(prev *Caches) string
	Copy() *Caches
}
