package bptree

import (
	"errors"
	"os"
	"github.com/drdreyworld/smconv"
)

type TreeFile struct {
	tree *Tree
	file *os.File
	save chan *Leaf
	stop chan bool

	pages int32

	keySize    int
	dataSize   int

}

func (tf *TreeFile) GetPagesCount() int32 {
	return tf.pages
}

func (tf *TreeFile) GetNextPageNum() int32 {
	tf.pages++
	return tf.pages-1
}

func (tf *TreeFile) GetKeySize() int {
	return tf.keySize
}

func (tf *TreeFile) GetDataSize() int {
	return tf.dataSize
}

func (tf *TreeFile) GetRowSize() int {
	return 8 + tf.keySize + tf.dataSize
}

func (tf *TreeFile) GetPageInfoSize() int {
	return 3 * 5 // index, next, count * 5 bytes
}

func (tf *TreeFile) GetPageSize() int {
	return tf.GetPageInfoSize() + tf.GetRowSize()*tf.tree.GetRowsCountInPage()
}

func (tf *TreeFile) GetPageOffset(page int32) int64 {
	return int64(int(page) * tf.GetPageSize())
}

func (tf *TreeFile) OpenFile(filename string) (err error) {
	if tf.file != nil {
		return errors.New("File is already opened")
	}

	tf.file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}

	err = tf.LoadTree()
	if err != nil {
		return err
	}

	tf.stop = make(chan bool)
	tf.save = make(chan *Leaf, 1000000)

	go func() {
		for leaf := range tf.save {
			tf.writeLeafToFile(leaf)
		}
		tf.stop <- true
	}()

	return nil
}

func (tf *TreeFile) LoadTree() error {

	pagesize := tf.GetPageSize()
	infosize := tf.GetPageInfoSize() + 4 + tf.keySize

	s, err := tf.file.Stat()
	if err != nil {
		return err
	}

	pagescount := int(s.Size() / int64(pagesize))

	leafs := make([]*Leaf, pagescount)
	fkeys := make([]Key, pagescount)
	nexts := make([]int, pagescount)
	pages := make([]int, pagescount)

	tf.tree.treeHeight = 0
	tf.tree.rowsCount = 0

	for i := 0; i < pagescount; i++ {

		offset := int64(i * pagesize)
		bytes := make([]byte, infosize)

		if _, err := tf.file.ReadAt(bytes, offset); err != nil {
			return err
		}

		leafs[i] = &Leaf{
			tree:  tf.tree,
			count: int(smconv.Int32FromBytes(bytes[10:15])),
			page:  int32(i),
			// @todo rows in leaf 2*n-1 || 2*n ?
			rows: make(Rows, tf.tree.Mid*2),
		}

		fkeys[i] = bytes[15+4 : 15+4+smconv.Uint32FromBytes(bytes[15:15+4])]

		nexts[i] = int(smconv.Int32FromBytes(bytes[5:10]))

		tf.tree.rowsCount += leafs[i].count
	}

	tf.pages = int32(pagescount)

	if len(leafs) == 0 {
		return nil
	}

	if len(leafs) == 1 {
		tf.tree.Root = leafs[0]
		tf.tree.treeHeight = 1
		return nil
	}

	for i := 0; i < len(leafs); i++ {
		if nexts[i] > -1 {
			leafs[i].next = leafs[nexts[i]]
			leafs[nexts[i]].prev = leafs[i]
		} else {
			tf.tree.lastLeaf = leafs[i]
		}
	}

	first := leafs[0]

	for first.prev != nil {
		first = first.prev
	}

	tf.tree.firstLeaf = first

	for i := 0; first != nil; first = first.next {
		pages[i] = int(first.page)
		i++
	}

	tf.tree.Root = tf.reconstructTree(pages, fkeys, leafs)
	tf.tree.treeHeight = 1

	for item := tf.tree.Root; ; tf.tree.treeHeight++ {
		if b, ok := item.(*Branch); ok {
			item = b.items[0]
		} else {
			break
		}
	}

	return nil
}

func (tf *TreeFile) DivAndRoundUp(x, y int) (z int) {
	z = x / y
	if x % y > 0 {
		z++
	}
	return z
}

func (tf *TreeFile) reconstructTree(pages []int, keys []Key, leafs []*Leaf) Item {
	var reconstructTreeFunc func(min, max int) Item

	itemsInBranch := tf.tree.GetRowsCountInPage()

	reconstructTreeFunc = func(min, max int) Item {

		pagesLength := (max - min)
		chunkSize := tf.DivAndRoundUp(pagesLength, itemsInBranch)
		chunksCount := tf.DivAndRoundUp(pagesLength, chunkSize)

		branch := tf.tree.createBranch()
		branch.keys = make(Keys, chunksCount-1)
		branch.items = make(Items, chunksCount)

		for i := 1; i < chunksCount; i++ {
			page := pages[min+(i*chunkSize)]
			branch.keys[i-1] = keys[page]
		}

		if chunkSize == 1 {
			for i := 0; i < chunksCount; i++ {
				page := pages[min+(i*1)]
				branch.items[i] = leafs[page]
			}
		} else {
			for i := 0; i < chunksCount; i++ {
				m := min + ((i + 0) * chunkSize)
				n := min + ((i + 1) * chunkSize)

				if n > max {
					n = max
				}

				branch.items[i] = reconstructTreeFunc(m, n)
			}
		}

		return branch
	}

	return reconstructTreeFunc(0, len(pages))
}

func (tf *TreeFile) writeLeafToFile(leaf *Leaf) {
	if tf.file != nil && leaf.IsChanged() {
		leaf.Lock()
		leaf.changed = false

		defer leaf.Unlock()

		n, err := tf.file.WriteAt(leaf.toBytes(), tf.GetPageOffset(leaf.page))
		if err != nil {
			panic(err)
		}

		if n != tf.GetPageSize() {
			panic("Invalid written bytes count!")
		}
	}
}

func (tf *TreeFile) readLeafFromFile(leaf *Leaf) {
	if tf.file != nil && !leaf.IsLoaded() {
		leaf.Lock()
		defer leaf.Unlock()

		bytes := make([]byte, tf.GetPageSize())

		n, err := tf.file.ReadAt(bytes, tf.GetPageOffset(leaf.page))
		if err != nil {
			panic(err)
		}

		if n != tf.GetPageSize() {
			panic("Invalid readed bytes count!")
		}

		leaf.FromBytes(bytes)
		leaf.loaded = true
	}
}

func (tf *TreeFile) CloseFile() error {
	if tf.file != nil {
		close(tf.save)
		<-tf.stop

		if err := tf.file.Sync(); err != nil {
			return err
		}
		return tf.file.Close()
	}
	return nil
}
