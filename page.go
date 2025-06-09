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


func (p *Page) Write(data []byte) {
	for i:=0;i<len(data);i++ {
			p.data[i] = data[i]
	}
	// for i:=0;i<PAGE_SIZE;i++ {
	// 	p.data[i] = 0
	// }
}