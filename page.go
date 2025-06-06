package main


type Page struct {
	pageId   int32
	pinCount int32
	isDirty  bool
	data     [PAGE_SIZE]byte
}


func (p *Page) GetData() [PAGE_SIZE]byte {
	return p.data
}

func (p *Page) Reset() {
	for i:=0;i<PAGE_SIZE;i++ {
		p.data[i] = 0
	}
}