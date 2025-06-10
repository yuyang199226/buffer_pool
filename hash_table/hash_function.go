package hashtable


import (
	"github.com/spaolacci/murmur3"
)


func Hash(key []byte) uint32 {
	// MurmurHash3 32-bit
	return murmur3.Sum32([]byte("Hello, world!"))

}
