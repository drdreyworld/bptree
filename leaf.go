package bptree

import (
	"sync"
	"github.com/drdreyworld/smconv"
)

type Leaf struct {
	sync.Mutex
	tree    *Tree
	rows    Rows
	page    int32
	next    *Leaf
	prev    *Leaf
	count   int
	loaded  bool
	changed bool
}

func (item *Leaf) IsFull() bool {
	return item.count > 2*item.tree.Mid-1
}

func (item *Leaf) IsChanged() bool {
	return item.changed
}

func (item *Leaf) IsLoaded() bool {
	return item.loaded
}

func (item *Leaf) Find(key Key) *Row {
	item.Load()
	item.Lock()
	defer item.Unlock()

	for i := 0; i < item.count; i++ {
		row := item.rows.GetRow(i)
		if key.Equal(row.Key()) {
			return row
		}
	}
	return nil
}

func (item *Leaf) Insert(row *Row) {
	item.Load()
	item.Lock()
	defer item.Unlock()

	item.changed = true
	key := row.Key()

	i := item.count
	for i > 0 {
		irow := item.rows.GetRow(i - 1)
		cmpr := key.Compare(irow.Key())

		if cmpr == EQUAL {
			item.rows.SetRow(i-1, row)
			return
		}

		if cmpr == MORE {
			break
		}

		item.rows.SetRow(i, irow)
		i--
	}

	item.rows.SetRow(i, row)
	item.count++
	item.tree.rowsCount++
}

func (item *Leaf) Split(parent *Branch) {
	n := item.tree.Mid
	if item.IsFull() {
		item.Load()
		item.Lock()

		leaf := item.tree.createLeaf()
		leaf.changed = true
		leaf.count = item.count - n
		copy(leaf.rows, item.rows[n:])

		copy(item.rows, item.rows[:n])
		item.count = n

		leaf.prev = item
		leaf.next = item.next
		item.next = leaf

		item.tree.lastLeaf = leaf

		i := parent.keys.Insert(leaf.rows[0].Key())

		parent.items = append(parent.items, nil)
		copy(parent.items[i+1:], parent.items[i:])

		parent.items[i] = item
		parent.items[i+1] = leaf

		item.Unlock()

		item.Save()
		leaf.Save()
	} else {
		item.Save()
	}
}

func (item *Leaf) Unload() {
	item.Lock()
	defer item.Unlock()

	if item.IsLoaded() {
		item.loaded = false
		item.rows = make(Rows, item.tree.Mid*2)
	}
}

func (item *Leaf) Load() {
	item.tree.file.readLeafFromFile(item)
}

func (item *Leaf) Save() {
	if item.tree.file.file != nil {
		item.tree.file.save <- item
	}
}

func (item *Leaf) ScanRowsASC(fn func(row *Row)) {
	item.Load()
	for i := 0; i < item.count; i++ {
		fn(item.rows.GetRow(i))
	}
}

func (item *Leaf) ScanRowsDESC(fn func(row *Row)) {
	item.Load()
	for i := item.count; i > 0; i-- {
		fn(item.rows.GetRow(i))
	}
}

func (item *Leaf) toBytes() []byte {
	page := make([]byte, item.tree.file.GetPageSize())

	pos := 0
	copy(page[pos:], smconv.Int32ToBytes(item.page))

	pos += 5
	if item.next != nil {
		copy(page[pos:], smconv.Int32ToBytes(item.next.page))
	} else {
		copy(page[pos:], smconv.Int32ToBytes(int32(-1)))
	}

	pos += 5
	copy(page[pos:], smconv.Int32ToBytes(int32(item.count)))

	pos += 5
	rsz := item.tree.file.GetRowSize()
	for i := 0; i < item.count; i++ {
		copy(page[pos:pos+rsz], *item.rows.GetRow(i))
		pos += rsz
	}

	return page
}

func (item *Leaf) FromBytes(bytes []byte) {
	item.count = int(smconv.Int32FromBytes(bytes[10:15]))

	rsz := item.tree.file.GetRowSize()
	pos := item.tree.file.GetPageInfoSize()

	for i := 0; i < item.count; i++ {
		row := Row(bytes[pos : pos+rsz])
		pos += rsz

		item.rows.SetRow(i, &row)
	}
}
