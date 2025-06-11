package page

import (
	"bufferpool/conf"
	"fmt"
)




type Page struct {
	PageId   int32
	PinCount int32
	IsDirty  bool
	data     [conf.PAGE_SIZE]byte
}


func (p *Page) GetData() [conf.PAGE_SIZE]byte {
	return p.data
}

func (p *Page) Reset() {
	for i:=0;i<conf.PAGE_SIZE;i++ {
		p.data[i] = 0
	}
}


func (p *Page) Write(data []byte) {
	for i:=0;i<len(data);i++ {
			p.data[i] = data[i]
	}
	// for i:=0;i<PAGE_SIZE;i++ {
	// 	p.data[i] = 0
	// }
}

/**
 *
 * Directory Page for extendible hash table.
 *
 * Directory format (size in byte):
 * --------------------------------------------------------------------------------------------
 * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
 * --------------------------------------------------------------------------------------------
 */
const DIRECTORY_ARRAY_SIZE = 512


type HashTableDirectoryPage struct {
	pageId int32
	lsn uint32
	globalDepth uint32
	localDepths [DIRECTORY_ARRAY_SIZE]uint8
	bucketPageIds [DIRECTORY_ARRAY_SIZE]int32

}


func (p *HashTableDirectoryPage) SetLsn(lsn uint32)  {
	p.lsn = lsn
}

func (p *HashTableDirectoryPage) SetPageId(id int32)  {
	p.pageId = id
}
func (p *HashTableDirectoryPage) GetBucketPageId(bucket_idx uint32)  int32{
	return p.bucketPageIds[bucket_idx]
}


func (p *HashTableDirectoryPage) SetBucketPageId(bucket_idx uint32, pageId int32)  {
	p.bucketPageIds[bucket_idx] = pageId
}

func (p *HashTableDirectoryPage) SetGlobalDepth(depth uint32)  {
	p.globalDepth = depth
}


func (p *HashTableDirectoryPage) GetGlobalDepthMask() uint32 {
	return (1<< p.globalDepth) - 1
}



func (p *HashTableDirectoryPage) GetGlobalDepth() uint32 {
	return p.globalDepth
}

func (p *HashTableDirectoryPage) GetLocalDepth(bucket_idx uint32) uint32 {
	return uint32(p.localDepths[bucket_idx])
}

func (p *HashTableDirectoryPage) SetLocalDepth(bucket_idx uint32, depth uint8)  {
	p.localDepths[bucket_idx] = depth
}


func (p *HashTableDirectoryPage) PrintDirectory() string {
	return fmt.Sprintf("%d, %d, %d %v", p.lsn, p.pageId, p.globalDepth, p.bucketPageIds)
}

func (p *HashTableDirectoryPage) IncrGlobalDepth() {
	p.globalDepth++
}

func (p *HashTableDirectoryPage) Size() uint32 {
	return 1 << p.globalDepth
}

