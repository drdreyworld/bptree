package bptree

import "github.com/drdreyworld/smconv"

func CreateRow(ks, ds int, key, data []byte) *Row {
	row := make(Row, 8+ks+ds)
	pos := 0

	copy(row[pos:pos+4], smconv.Uint32ToBytes(uint32(len(key))))
	pos += 4

	copy(row[pos:pos+ks], key)
	pos += ks

	copy(row[pos:pos+4], smconv.Uint32ToBytes(uint32(len(data))))
	pos += 4

	copy(row[pos:pos+ds], data)
	return &row
}

type Row []byte // keySize (4byte) -> key -> dataSize (4byte) -> data

func (row *Row) CompareKeyWith(key Key) int {
	kl := smconv.Uint32FromBytes((*row)[0:4])
	kd := Key((*row)[4 : 4+kl])
	return kd.Compare(key)
}

func (row *Row) Key() Key {
	kl := smconv.Uint32FromBytes((*row)[0:4])
	return Key((*row)[4 : 4+kl])
}

type Rows []*Row

func (rows *Rows) SetRow(i int, row *Row) {
	(*rows)[i] = row
}

func (rows *Rows) GetRow(i int) *Row {
	return (*rows)[i]
}
