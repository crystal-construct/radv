// Licensed to the public under one or more agreements.
// Crystal Construct Limited licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

package database

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger"
)

type Triplestore struct {
	db    *badger.DB
	iopts badger.IteratorOptions
}

type FieldPrefix []byte

var (
	dbEmpty FieldPrefix = []byte{}
	dbKey   FieldPrefix = []byte{0}
	dbSPO   FieldPrefix = []byte{1}
	dbPOS   FieldPrefix = []byte{2}
	dbSOP   FieldPrefix = []byte{3}
	dbValue FieldPrefix = []byte{4}
)

type Field int

var (
	Subject   Field = 0
	Predicate Field = 1
	Object    Field = 2
)

func New() *Triplestore {
	opts := badger.DefaultOptions
	opts.Dir = "data"
	opts.ValueDir = "data"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	var iopts = badger.DefaultIteratorOptions
	//iopts.PrefetchValues = false
	return &Triplestore{
		db:    db,
		iopts: iopts,
	}
}

func (t *Triplestore) Close() {
	t.db.Close()
}

func (t *Triplestore) Get(subject interface{}, predicate interface{}, object interface{}) ([][]byte, error) {
	txn := t.db.NewTransaction(false)
	defer txn.Discard()

	sarr, err := t.toKeys(txn, subject, false)
	if err != nil {
		return nil, err
	}

	parr, err := t.toKeys(txn, predicate, false)
	if err != nil {
		return nil, err
	}

	oarr, err := t.toKeys(txn, object, false)
	if err != nil {
		return nil, err
	}
	res := make([][]byte, 0)
	triples := make(chan []byte)
	go t.get(txn, sarr, parr, oarr, triples)
	for {
		tr, more := <-triples
		if more {
			kr := tr[19:]
			res = append(res, kr)
		} else {
			break
		}
	}
	return res, nil
}

func (t *Triplestore) get(txn *badger.Txn, sarr [][]byte, parr [][]byte, oarr [][]byte, triples chan []byte) {

	for _, si := range sarr {
		for _, pi := range parr {
			for _, oi := range oarr {
				var prefix []byte
				it := txn.NewIterator(t.iopts)
				defer it.Close()
				switch {
				case si != nil && pi != nil && oi != nil:
					prefix = zcopy(dbSPO, si, pi, oi)
				case si != nil && pi != nil:
					prefix = zcopy(dbSPO, si, pi)
				case pi != nil && oi != nil:
					prefix = zcopy(dbPOS, pi, oi)
				case si != nil && oi != nil:
					prefix = zcopy(dbSOP, si, oi)
				}
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					triples <- []byte(it.Item().Key())
				}
				it.Close()
			}
		}
	}

	close(triples)
	return
}

func (t *Triplestore) toKeys(txn *badger.Txn, input interface{}, create bool) ([][]uint8, error) {
	var res [][]uint8
	switch input.(type) {
	case [][]uint8:
		res = input.([][]uint8)
	case []uint8:
		res = [][]uint8{input.([]uint8)}
	case nil:
		res = [][]byte{nil}
	default:
		s, err := marshal(input)
		if err != nil {
			return nil, err
		}
		si, err := t.getId(txn, s, create)
		if err != nil {
			return nil, err
		}
		return [][]uint8{si}, nil
	}
	return res, nil
}

func (t *Triplestore) Put(subject interface{}, predicate interface{}, object interface{}) error {

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	s, err := marshal(subject)
	if err != nil {
		return err
	}
	p, err := marshal(predicate)
	if err != nil {
		return err
	}
	o, err := marshal(object)
	if err != nil {
		return err
	}

	si, err := t.getId(txn, s, true)
	if err != nil {
		return err
	}
	pi, err := t.getId(txn, p, true)
	if err != nil {
		return err
	}
	oi, err := t.getId(txn, o, true)
	if err != nil {
		return err
	}

	err = t.put(txn, si, pi, oi)
	if err != nil {
		return err
	}

	txn.Commit(func(e error) {
		err = e
	})
	return err
}
func (t *Triplestore) put(txn *badger.Txn, si []byte, pi []byte, oi []byte) error {

	spo := zcopy(dbSPO, si, pi, oi)
	sop := zcopy(dbSOP, si, oi, pi)
	pos := zcopy(dbPOS, pi, oi, si)
	err := txn.Set(spo, []byte{})
	if err != nil {
		return err
	}
	err = txn.Set(sop, []byte{})
	if err != nil {
		return err
	}
	err = txn.Set(pos, []byte{})
	if err != nil {
		return err
	}
	return nil
}

func zcopy(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

func (t *Triplestore) getId(txn *badger.Txn, value []byte, create bool) ([]byte, error) {
	dval := append(dbValue, value...)
	ret := make([]byte, 9)

	rawid, err := txn.Get(dval)
	if err != badger.ErrKeyNotFound {
		err2 := rawid.Value(func(val []byte) {
			ret = val
		})
		if err2 != nil {
			return nil, err2
		}
		return ret, nil
	}

	if err != badger.ErrKeyNotFound || !create {
		return nil, err
	}

	seq, seqerr := t.db.GetSequence(dbValue, 10)
	if seqerr != nil {
		return nil, seqerr
	}
	key, err := seq.Next()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, key)
	ret = append(dbKey, raw...)
	seq.Release()
	err = txn.Set(ret, dval)
	if err != nil {
		return nil, err
	}
	err = txn.Set(dval, ret)
	if err != nil {
		return nil, err
	}
	return ret, err
}

func (t *Triplestore) Materialize(keys [][]byte) []interface{} {
	res := make([]interface{}, 0)
	err := t.db.View(func(txn *badger.Txn) error {
		for _, i := range keys {
			value, err := txn.Get(i)
			if err != nil {
				return err
			}
			value.Value(func(val []byte) {
				rval := unmarshal(val[1:])
				res = append(res, rval)
			})
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return res
}
