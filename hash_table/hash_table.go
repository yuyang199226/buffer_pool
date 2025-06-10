package hashtable

import (
	bufferpool_manager "bufferpool/buffer_pool"
	"bufferpool/page"
	"bytes"
	"encoding/binary"
	"fmt"
)

type KeyType string 
type ValueType string


type HashFunc func(key []byte) uint32

type HashTable struct{
	bpm *bufferpool_manager.BufferPoolManager
	hashfunc HashFunc

	dirPageId int32
	globalDepth uint32
	buckets []uint32

}

func NewHashTable(bpm *bufferpool_manager.BufferPoolManager, hash_func HashFunc) *HashTable{
	ht := new(HashTable)
	ht.bpm = bpm
	ht.hashfunc = hash_func
	var (
		lsn uint32 = 0
		depth uint32 = 1
	)
	length := 1<< depth
	buckets := make([]uint32, length)
	var pageId int32
	p := ht.bpm.NewPageImpl(&pageId)
	dirPage := &page.HashTableDirectoryPage{}
	dirPage.SetLsn(0)
	dirPage.SetGlobalDepth(0)
	dirPage.SetPageId(pageId)
	content,err := StructToBytes(*dirPage)
	if err != nil {
		panic(err)
	}


	ht.buckets = buckets
	p.Write(content)
	bpm.UnpinPageImpl(pageId, true)
	fmt.Println(lsn, depth)
	
	return &HashTable{dirPageId: pageId}
}

func (ht *HashTable) Insert(key KeyType, val ValueType) {
	dp := ht.FetchDirectoryPage()
	bucket_idx := ht.keyToDirectoryIndex(key, dp)
	bucket_page_id := dp.GetBucketPageId(bucket_idx)
	ht.bpm.FetchPageImpl(bucket_page_id)

}

func (ht *HashTable) FetchDirectoryPage() *page.HashTableDirectoryPage {
	p := ht.bpm.FetchPageImpl(ht.dirPageId)
	data := p.GetData()
	dp := new(page.HashTableDirectoryPage)
	buf := bytes.NewReader(data[:])
	binary.Read(buf, binary.LittleEndian, dp)

	fmt.Println(dp.PrintDirectory())
	return dp








}


 /**
   * KeyToDirectoryIndex - maps a key to a directory index
   *
   * In Extendible Hashing we map a key to a directory index
   * using the following hash + mask function.
   *
   * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
   *
   * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
   * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
   * representation.
   *
   * @param key the key to use for lookup
   * @param dir_page to use for lookup of global depth
   * @return the directory index
   */
func (ht *HashTable) keyToDirectoryIndex(key KeyType, dir_page *page.HashTableDirectoryPage) uint32 {
	hash := ht.hash(key)
	index := hash & dir_page.GetGlobalDepthMask()
	return index
}











func (ht *HashTable) hash(key KeyType) uint32 {
	return ht.hashfunc([]byte(key))
}


func intToBytesLittleEndian(num uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(num))
	return bytes
}

func bytesToUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func StructToBytes(p page.HashTableDirectoryPage) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, &p)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}