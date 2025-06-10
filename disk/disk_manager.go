package disk

import (
	"container/list"
	"os"
	"bufferpool/conf"
)


type DiskManager struct {
	db string
	// pages map[int32][PAGE_SIZE]byte
	page map[int32]int32
	fd *os.File
	freeSlot *list.List
}

func NewDiskManager(db string) *DiskManager {
	fd, err := os.OpenFile(db, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	
	return &DiskManager{db: db, 
		page: make(map[int32]int32),
		fd: fd,
		freeSlot: list.New(),
	}
}


func (d *DiskManager) WritePage(id int32, b [conf.PAGE_SIZE]byte) {
	offset,ok := d.page[id]
	if !ok {
		offset = d.AllocatePage()
		d.page[id] = offset
	}
	//d.fd.Seek(int64(offset), 0)
	c := b[:]
	d.fd.WriteAt(c, int64(offset))
	d.fd.Sync()
	//d.pages[id] = b

}

func (d *DiskManager) AllocatePage() int32 {
	if d.freeSlot.Len() > 0 {
		node := d.freeSlot.Front()
		offset := node.Value.(int32)
		d.freeSlot.Remove(node)
		return offset
	}
	return int32(len(d.page) * conf.PAGE_SIZE)

}





func (d *DiskManager) ReadPage(pageId int32, data *[conf.PAGE_SIZE]byte) {
	offset,ok := d.page[pageId]
	if !ok {
		return

	}
	b := make([]byte, conf.PAGE_SIZE)
	n, err := d.fd.ReadAt(b, int64(offset))
	if err != nil {
		panic(err)
	}
	for i:=0;i<n;i++ {
		(*data)[i] = b[i]
	}
	// d.fd.Seek(0,0)
	//*data = d.pages[pageId]

}

func (d *DiskManager) DeallocatePage(pageId int32) {
	offset,find := d.page[pageId]
	if !find {
		// 表示这个page 没新建过，或者已经被删除了
		return 
	}
	d.freeSlot.PushBack(offset)
	delete(d.page, pageId)
}

func (d *DiskManager) Close() {
	d.fd.Close()

}