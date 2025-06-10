package hashtable

import bufferpool_manager "bufferpool/buffer_pool"


type HashTable struct{}

func NewHashTable(bpm *bufferpool_manager.BufferPoolManager) *HashTable{
	return &HashTable{}
}

func (ht *HashTable) Insert() {
	
}