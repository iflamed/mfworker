package storage

import (
	"bytes"
	"testing"
)

func TestNewMemoryStorage(t *testing.T) {
	s := NewMemoryStorage(100)
	str := []byte("the test string")
	s.Push(str)
	length := s.Length()
	if length != 1 {
		t.Errorf("The length should be 1.")
	}
	value := s.Shift()
	if !bytes.Equal(value, str) {
		t.Errorf("The strings should be equal, exptect %s bug got %s", str, value)
	}
	length = s.Length()
	if length != 0 {
		t.Errorf("The length should be 0.")
	}
	s.Close()
}