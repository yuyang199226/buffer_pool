package page

import "bufferpool/conf"




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