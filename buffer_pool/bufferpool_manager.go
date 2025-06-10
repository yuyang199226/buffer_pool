package bufferpool_manager

import (
	"container/list"
	"fmt"
	"bufferpool/conf"
	"bufferpool/disk"
	"bufferpool/page"
)







type BufferPoolManager struct {
	Pages         []page.Page
	PageTable     map[int32]int32
	FreeFrameList *list.List
	diskm         *disk.DiskManager
	replacer      *LruReplacer
	nextPageId    int32
}

func NewBufferPoolManager(poolSize int, diskManager *disk.DiskManager) *BufferPoolManager {
	bpm := new(BufferPoolManager)
	bpm.Pages = make([]page.Page, poolSize)
	bpm.PageTable = make(map[int32]int32)
	bpm.FreeFrameList = list.New()
	bpm.diskm = diskManager
	bpm.replacer = NewLruReplacer()
	for i := 0; i < poolSize; i++ {
		bpm.FreeFrameList.PushBack(int32(i))
	}

	return bpm
}

func (bpm *BufferPoolManager) FetchPageImpl(pageId int32) *page.Page {
	frameid, find := bpm.PageTable[pageId]
	if find {
		bpm.Pages[frameid].PinCount++
		bpm.replacer.Pin(frameid)
		return &bpm.Pages[frameid]
	}
	// 如果bufferpool 里的page 都有人在用，那就不能从disk 加载了，所以返回nil
	usedPage := 0
	for i := 0; i < len(bpm.Pages); i++ {
		if bpm.Pages[i].PinCount >= 1 {
			usedPage++
		}
	}
	if usedPage == len(bpm.Pages) {
		return nil
	}
	// bufferpool 没有找到，需要找到一个空闲的frame
	existFrame := false
	var newFrameId int32 = -1
	if bpm.FreeFrameList.Len() > 0 {
		newFrameId = bpm.FreeFrameList.Front().Value.(int32)
		bpm.FreeFrameList.Remove(bpm.FreeFrameList.Front())
		existFrame = true

	} else {
		ok := bpm.replacer.Victim(&newFrameId)
		if ok {
			existFrame = true
		}
	}
	if !existFrame {
		return nil
	}
	page := bpm.Pages[newFrameId]
	if page.IsDirty {
		// 写回disk
		bpm.FlushPageImpl(page.PageId)
	}
	bpm.PageTable[pageId] = newFrameId
	bpm.replacer.Pin(newFrameId)
	var data [conf.PAGE_SIZE]byte
	bpm.diskm.ReadPage(pageId, &data)
	bpm.Pages[newFrameId].PageId = pageId
	bpm.Pages[newFrameId].PinCount = 1
	bpm.Pages[newFrameId].IsDirty = false
	bpm.Pages[newFrameId].Write(data[:])
	return &bpm.Pages[newFrameId]
}

func (bpm *BufferPoolManager) NewPageImpl(pageId *int32) *page.Page {
	// 如果池子里的page 都有人在用,那没法新建了
	usedPage := 0
	for i := 0; i < len(bpm.Pages); i++ {
		if bpm.Pages[i].PinCount >= 1 {
			usedPage++
		}
	}
	if usedPage == len(bpm.Pages) {
		return nil
	}

	// 找一个空闲的frame 来装page
	existFrame := false
	var newFrameId int32 = -1
	if bpm.FreeFrameList.Len() > 0 {
		newFrameId = bpm.FreeFrameList.Front().Value.(int32)
		bpm.FreeFrameList.Remove(bpm.FreeFrameList.Front())

		existFrame = true

	} else {
		ok := bpm.replacer.Victim(&newFrameId)
		if ok {
			existFrame = true
		}
		if bpm.Pages[newFrameId].IsDirty {
		// 写回disk
		bpm.FlushPageImpl(bpm.Pages[newFrameId].PageId)
		}
			if bpm.Pages[newFrameId].PageId != conf.INVALID_PAGE_ID {

		delete(bpm.PageTable, bpm.Pages[newFrameId].PageId)
	}
	}
	if !existFrame {
		return nil
	}


	*pageId = bpm.AllocatePage()
	bpm.replacer.Pin(newFrameId)
	bpm.Pages[newFrameId].PageId = *pageId
	bpm.Pages[newFrameId].IsDirty = false
	bpm.Pages[newFrameId].PinCount = 1
	bpm.Pages[newFrameId].Reset()
	bpm.PageTable[*pageId] = newFrameId
	return &bpm.Pages[newFrameId]
}

/*
Implementation of unpin page
if pin_count>0, decrement it and if it becomes zero, put it back to
replacer if pin_count<=0 before this call, return false. is_dirty: set the
dirty flag of this page
*/
func (bpm *BufferPoolManager) UnpinPageImpl(pageId int32, isDirty bool) bool {
	frameId, find := bpm.PageTable[pageId]
	if !find {

		return false
	}

	page := &bpm.Pages[frameId]
	if page.PinCount <= 0 {
		return false
	}
	page.IsDirty = isDirty
	page.PinCount--

	if page.PinCount == 0 {
		bpm.replacer.Unpin(frameId)
	}
	return true

}

// return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
// 从buffer_pool 删除
func (bpm *BufferPoolManager) DeletePageImpl(pageId int32) bool {
	frameId, find := bpm.PageTable[pageId]
	if !find {
		return true
	}
	page := bpm.Pages[frameId]
	if page.PinCount > 0 {
		return false
	}
	page.PageId = conf.INVALID_PAGE_ID
	page.IsDirty = false
	page.Reset()
	delete(bpm.PageTable, pageId)
	bpm.FreeFrameList.PushBack(frameId)
	// delete from fisk file
	bpm.diskm.DeallocatePage(pageId)
	return true
}

func (bpm *BufferPoolManager) FlushALlPageImpl(pageId int32) bool {
	for i := 0; i < len(bpm.Pages); i++ {
		bpm.FlushPageImpl(bpm.Pages[i].PageId)
	}
	frameId, find := bpm.PageTable[pageId]
	if !find {
		return false
	}
	page := bpm.Pages[frameId]
	bpm.diskm.WritePage(page.PageId, page.GetData())
	if page.IsDirty {
		page.IsDirty = false
	}
	return true
}
func (bpm *BufferPoolManager) FlushPageImpl(pageId int32) bool {
	frameId, find := bpm.PageTable[pageId]
	if !find {
		return false
	}
	page := bpm.Pages[frameId]
	// 表示已经是被删除了，所以不需要flush 了
	if page.PageId == conf.INVALID_PAGE_ID {
		return false
	}
	bpm.diskm.WritePage(page.PageId, page.GetData())
	if page.IsDirty {
		page.IsDirty = false
	}
	return true
}

func (bpm *BufferPoolManager) AllocatePage() int32 {
	nextPageId := bpm.nextPageId
	bpm.nextPageId++
	return nextPageId

}

func (bpm *BufferPoolManager) PrintPage() {
	for _, v := range bpm.Pages {
		fmt.Printf("pageid=%d, pincount=%d\n", v.PageId, v.PinCount)
	}
	fmt.Println(bpm.PageTable)
}

