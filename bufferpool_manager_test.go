package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferpool(t *testing.T) {
	assert := assert.New(t)
	var temp_page_id_t int32
	dm := NewDiskManager("test.db")
	bpm := NewBufferPoolManager(10, dm)
	page_zero := bpm.NewPageImpl(&temp_page_id_t)
	assert.Equal(int32(0), temp_page_id_t, "should be equal")
	assert.NotNil(page_zero)
	// change content in page one

	word := []byte("hello")
	n := len(word)
	page_zero.Write(word)
	fmt.Println("-------------111111")
	dd := page_zero.GetData()
	//fmt.Println(string(dd[:n]))
	// strcpy(page_zero->GetData(), "Hello");
	var temp_page_id int32
	for i := 1; i < 10; i++ {
		assert.NotNil(bpm.NewPageImpl(&temp_page_id))
	}

	/// all the pages are pinned, the buffer pool is full
	for i := 10; i < 15; i++ {
		assert.Nil(bpm.NewPageImpl(&temp_page_id_t))
	}
	// upin the first five pages, add them to LRU list, set as dirty
	for i := 0; i < 5; i++ {

		assert.Equal(true, bpm.UnpinPageImpl(int32(i), true), fmt.Sprintf("i=%d", i))
	}

	for i := 10; i < 14; i++ {
		assert.NotNil(bpm.NewPageImpl(&temp_page_id_t))
	}
	fmt.Println("==============")
	page_zero = bpm.FetchPageImpl(0)

	dd = page_zero.GetData()
	fmt.Println(string(dd[:n]))
	bpm.diskm.Close()

}

func TestBufferpool2(t *testing.T) {
	assert := assert.New(t)
	var temp_page_id_t int32
	dm := NewDiskManager("test.db")
	bpm := NewBufferPoolManager(10, dm)
	page_zero := bpm.NewPageImpl(&temp_page_id_t)
	assert.Equal(int32(0), temp_page_id_t, "should be equal")
	assert.NotNil(page_zero)
	// change content in page one
	word := []byte("hello")
	n := len(word)

	page_zero.Write(word)

	dd := page_zero.GetData()
	fmt.Println(string(dd[:n]))
	// strcpy(page_zero->GetData(), "Hello");
	var temp_page_id int32
	for i := 1; i < 10; i++ {
		assert.NotNil(bpm.NewPageImpl(&temp_page_id))
	}

	for i := 0;i<1;i++ {
		assert.Equal(true, bpm.UnpinPageImpl(int32(i), true))
		page_zero = bpm.FetchPageImpl(0)
		data := page_zero.GetData()
		text := string(data[:n])
		assert.Equal(text, "hello")
		assert.Equal(true, bpm.UnpinPageImpl(int32(i), true))
		assert.NotNil(bpm.NewPageImpl(&temp_page_id))
		t.Log(temp_page_id)
	}

	testls := []int32{5,6,7,8,9,10}
	for _,v := range testls {
		page := bpm.FetchPageImpl(v)
		if page == nil {
			t.Logf("v = %d false nil",v)
			return
		}
		assert.Equal(v, page.pageId)
		bpm.UnpinPageImpl(v, true)
	}

	bpm.UnpinPageImpl(10, true)
	page_zero  = bpm.FetchPageImpl(0)
		data := page_zero.GetData()
		text := string(data[:n])
		assert.Equal("hello", text)
}
