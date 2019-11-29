package storage

type Bucket interface {
	Push(value []byte) bool
	Shift() []byte
	Length() uint64
	Close()
}
