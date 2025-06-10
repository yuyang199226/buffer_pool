package bufferpool_manager

import "testing"

func TestLruReplacer(t *testing.T) {
	rep := NewLruReplacer()

	rep.Unpin(1)
	rep.Unpin(2)
	rep.Unpin(4)
	rep.Unpin(6)
	rep.Pin(2)
	var frameId int32
	rep.Victim(&frameId)
	t.Log(frameId)
	t.Log(rep.Size())
	t.Log(rep.frameList.Front().Value)
	t.Log(rep.frameList.Back().Value)
}