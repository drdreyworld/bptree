package bptree

import (
	"bytes"
)

type Key []byte

const EQUAL = 0
const MORE = +1
const LESS = -1

func (self *Key) Less(key Key) bool {
	return self.Compare(key) == LESS
}

func (self *Key) More(key Key) bool {
	return self.Compare(key) == MORE
}

func (self *Key) Equal(key Key) bool {
	return self.Compare(key) == EQUAL
}


func (self *Key) Compare(key Key) int {
	return bytes.Compare(*self, key)
}

type Keys []Key

func (keys *Keys) Insert(key Key) int {
	i := 0
	for ; i < len(*keys); i++ {
		cmpr := key.Compare((*keys)[i])

		if cmpr == EQUAL {
			return i
		}

		if cmpr == LESS {
			break
		}
	}

	(*keys) = append(*keys, Key{})

	copy((*keys)[i+1:], (*keys)[i:])

	(*keys)[i] = key

	return i
}