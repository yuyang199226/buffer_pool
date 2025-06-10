package bufferpool_manager

import (
	"container/list"
)

type LruReplacerIn interface {
	Victim(*int32) bool
	Pin(frameId int32)
	Unpin(frameId int32)
	Size() int
}

type LruReplacer struct {
	frameTable map[int32]*list.Element
	frameList  *list.List
}

func NewLruReplacer() *LruReplacer {
	return &LruReplacer{
		frameTable: make(map[int32]*list.Element),
		frameList:  list.New(),
	}
}

// pin 说明要被引用，所以从这里删掉
func (r *LruReplacer) Pin(frameId int32) bool {
	if _, find := r.frameTable[frameId]; !find {
		return false
	}
	r.frameList.Remove(r.frameTable[frameId])
	delete(r.frameTable, frameId)
	return true
}

// 驱逐出一个来frame_id
func (r *LruReplacer) Victim(frameId *int32) bool {
	if r.Size() == 0 {
		return false
	}
	val, _ := r.frameList.Front().Value.(int32)
	*frameId = val
	r.frameList.Remove(r.frameList.Front())
	delete(r.frameTable, val)
	return true
}

// 增加
func (r *LruReplacer) Unpin(frameId int32) {
	if _, find := r.frameTable[frameId]; find {
		return
	}
	ele := r.frameList.PushBack(frameId)
	r.frameTable[frameId] = ele
}

func (r *LruReplacer) Size() int {
	return len(r.frameTable)
}
