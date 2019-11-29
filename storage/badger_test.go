package storage

import (
	"bytes"
	"testing"
)

func TestNewBadgerStorage(t *testing.T) {
	s, err := NewBadgerStorage("../queue.db", nil)
	if err != nil {
		t.Errorf("Create storage failed. %s", err)
	}
	str := []byte("test string")
	s.Push(str)
	length := s.Length()
	if length != 1 {
		t.Errorf("The queue storage length is not right expect 1, fact %d", length)
	}
	value := s.Shift()
	if !bytes.Equal(str, value) {
		t.Errorf("The value is not equal (%s, %s)", str, value)
	}
	length = s.Length()
	if length != 0 {
		t.Errorf("The queue storage length is not right expect 0, fact %d", length)
	}
	s.Close()
}