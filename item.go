package bptree

type Items []Item

type Item interface {
	IsFull() bool

	Find(key Key) *Row
	Insert(row *Row)

	Split(branch *Branch)
}