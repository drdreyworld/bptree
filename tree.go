package bptree

type Tree struct {
	Root Item
	Mid  int
	file *TreeFile

	treeHeight int
	rowsCount  int

	firstLeaf *Leaf
	lastLeaf  *Leaf
}

func (tree *Tree) Init(keySize, dataSize int) {
	tree.file = &TreeFile{
		tree:     tree,
		keySize:  keySize,
		dataSize: dataSize,
	}
}

func (tree *Tree) createLeaf() *Leaf {
	return &Leaf{
		tree:   tree,
		page:   tree.file.GetNextPageNum(),
		rows:   make(Rows, tree.Mid*2),
		loaded: true,
	}
}

func (tree *Tree) createBranch() *Branch {
	return &Branch{tree: tree}
}

func (tree *Tree) Insert(row *Row) {
	if tree.Root == nil {
		tree.treeHeight = 1
		leaf := tree.createLeaf()

		tree.Root = leaf
		tree.firstLeaf = leaf
		tree.lastLeaf = leaf
	}

	tree.Root.Insert(row)

	if tree.Root.IsFull() {
		branch := tree.createBranch()
		branch.items = Items{tree.Root}
		tree.Root.Split(branch)
		tree.Root = branch
		tree.treeHeight++
	} else {
		if leaf, ok := tree.Root.(*Leaf); ok {
			leaf.Save()
		}
	}
}

func (tree *Tree) Find(key Key) *Row {
	if tree.Root == nil {
		return nil
	}

	return tree.Root.Find(key)
}

func (tree *Tree) GetHeight() int {
	return tree.treeHeight
}

func (tree *Tree) GetRowsCount() int {
	return tree.rowsCount
}

func (tree *Tree) GetRowsCountInPage() int {
	return (2 * tree.Mid) - 1
}

func (tree *Tree) ScanLeafs(fn func(row *Leaf) bool, asc bool) {
	var leaf *Leaf

	if asc {
		for leaf = tree.firstLeaf; leaf != nil && fn(leaf); leaf = leaf.next {
		}
	} else {
		for leaf = tree.lastLeaf; leaf != nil && fn(leaf); leaf = leaf.prev {
		}
	}
}

func (tree *Tree) ScanRows(fn func(row *Row) bool, asc bool) {
	tree.ScanLeafs(func(leaf *Leaf) bool {
		if asc {
			return leaf.ScanRowsASC(fn)
		} else {
			return leaf.ScanRowsDESC(fn)
		}
	}, asc)
}

func (tree *Tree) OpenFile(filename string) error {
	return tree.file.OpenFile(filename)
}

func (tree *Tree) CloseFile() error {
	return tree.file.CloseFile()
}
