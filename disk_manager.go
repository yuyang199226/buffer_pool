package main

import (
	"fmt"
	"os"
)


type DiskManager struct {
	db string
	// pages map[int32][PAGE_SIZE]byte
	page map[int32]int32
	fd *os.File
}

func NewDiskManager(db string) *DiskManager {
	fd, err := os.OpenFile(db, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	
	return &DiskManager{db: db, 
		page: make(map[int32]int32),
		fd: fd,
	}
}


func (d *DiskManager) WritePage(id int32, b [PAGE_SIZE]byte) {
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
	return int32(len(d.page) * PAGE_SIZE)

}





func (d *DiskManager) ReadPage(pageId int32, data *[PAGE_SIZE]byte) {
	offset,ok := d.page[pageId]
	if !ok {
		return

	}
	fmt.Println(d.page, offset)
	b := make([]byte, 0)
	n, err := d.fd.ReadAt(b, int64(offset))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
	for i:=0;i<n;i++ {
		(*data)[i] = b[i]
	}
	//*data = d.pages[pageId]

}

func (d *DiskManager) DeallocatePage(pageId int32) {
	panic("not impl")
}

func (d *DiskManager) Close() {
	d.fd.Close()

}