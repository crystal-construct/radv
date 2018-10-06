// Licensed to the public under one or more agreements.
// Crystal Construct Limited licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

package database

import (
	"crypto/sha256"
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger"
)

type Triplestore struct {
	db    *badger.DB
	iopts badger.IteratorOptions
}

const hashKeySpace = uint64(9223372036854775807)

type FieldPrefix []byte

var (
	dbEmpty FieldPrefix = make([]byte, 0)

	// Prefixes for different kinds of database keys
	dbKey     FieldPrefix = []byte{0}
	dbSPO     FieldPrefix = []byte{1}
	dbPOS     FieldPrefix = []byte{2}
	dbSOP     FieldPrefix = []byte{3}
	dbValue   FieldPrefix = []byte{4}
	dbHash    FieldPrefix = []byte{5}
	dbHashKey FieldPrefix = []byte{6}
)

// Constants to describe fields
type Field int

var (
	Subject   Field = 0
	Predicate Field = 1
	Object    Field = 2
)

// New creates a new triplestore
func New(directory string) *Triplestore {
	opts := badger.DefaultOptions
	opts.Dir = directory
	opts.ValueDir = directory

	// Open a connection to the underlying key value database
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	var iopts = badger.DefaultIteratorOptions

	return &Triplestore{
		db:    db,
		iopts: iopts,
	}
}

// Close closes the underlying vey value database
func (t *Triplestore) Close() {
	t.db.Close()
}

// Get performs a simple query against the triplestore.  Set one of the parameters to nil to query for that.
func (t *Triplestore) Get(subject interface{}, predicate interface{}, object interface{}) ([][]byte, error) {
	// Create a read-only transaction
	txn := t.db.NewTransaction(false)
	defer txn.Discard()

	//Convert subject, predicate and object into [][]byte{} containing a list of keys
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

	//Create a results array, and a channel to receive the results stream
	res := make([][]byte, 0)
	triples := make(chan []byte)

	// Fire off the query, and collate the results
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

	// Loop through the cartesian combinations of keys
	for _, si := range sarr {
		for _, pi := range parr {
			for _, oi := range oarr {
				var prefix []byte
				it := txn.NewIterator(t.iopts)
				// work out which prefix to use for the key lookup
				switch {
				case si != nil && pi != nil && oi != nil:
					prefix = multiAppend(dbSPO, si, pi, oi)
				case si != nil && pi != nil:
					prefix = multiAppend(dbSPO, si, pi)
				case pi != nil && oi != nil:
					prefix = multiAppend(dbPOS, pi, oi)
				case si != nil && oi != nil:
					prefix = multiAppend(dbSOP, si, oi)
				}

				//Loop through and stream the results
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
	// Homogenize the input to an array of keys
	switch input.(type) {
	case [][]uint8:
		// Already in the right format
		res = input.([][]uint8)
	case []uint8:
		// A single key - turn this into a 1 element array
		res = [][]uint8{input.([]uint8)}
	case nil:
		// If nil, return an array with a nil member
		res = [][]byte{nil}
	default:
		// If this is a value, convert it to binary, and get it's ID
		s, err := marshal(input)
		if err != nil {
			return nil, err
		}
		si, err := t.getID(txn, s, create)
		if err != nil {
			return nil, err
		}
		return [][]uint8{si}, nil
	}
	return res, nil
}

// Put writes a triple to the triplestore
func (t *Triplestore) Put(subject interface{}, predicate interface{}, object interface{}) error {

	// Create an update transaction
	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	// convert all input values to binary
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

	// Get the ID for each input value
	si, err := t.getID(txn, s, true)
	if err != nil {
		return err
	}
	pi, err := t.getID(txn, p, true)
	if err != nil {
		return err
	}
	oi, err := t.getID(txn, o, true)
	if err != nil {
		return err
	}

	// Write the values to the database
	err = t.put(txn, si, pi, oi)
	if err != nil {
		return err
	}

	// Commit the transaction
	txn.Commit(func(e error) {
		err = e
	})
	return err
}
func (t *Triplestore) put(txn *badger.Txn, si []byte, pi []byte, oi []byte) error {

	// The 3 conbinationss of keys we store
	combinations := [][][]byte{
		{dbSPO, si, pi, oi},
		{dbSOP, si, oi, pi},
		{dbPOS, pi, oi, si},
	}
	for _, i := range combinations {
		// Write each combination
		err := txn.Set(multiAppend(i...), []byte{})
		if err != nil {
			return err
		}
	}
	return nil
}

// multiAppend combines byte slices together
func multiAppend(slices ...[]byte) []byte {
	var length int
	for _, s := range slices {
		length += len(s)
	}
	tmp := make([]byte, length)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

// getID works out the correct id to use for a value
func (t *Triplestore) getID(txn *badger.Txn, value []byte, create bool) ([]byte, error) {
	var v, seek []byte
	var rawid *badger.Item
	var err error
	var hash []byte

	// Prepend the value with our value identifier
	v = append(dbValue, value...)
	if len(value) > 39 {
		// If the value is 40 bytes or more (including the identifier) compute the hash to use as a lookup
		h := sha256.Sum256(v)
		hash = h[:]
		seek = append(dbHash, hash...)
	} else {
		// If the value is shorter the 40 bytes just store the value
		seek = v
	}
	ret := make([]byte, 9)

	// Try and get the key for the value from the database
	rawid, err = txn.Get(seek)

	if err != badger.ErrKeyNotFound {
		if err != nil {
			// Error for any error apart from key not found
			return nil, err
		}

		// We have a key to return
		err2 := rawid.Value(func(val []byte) error {
			ret = val
			return nil
		})
		if err2 != nil {
			return nil, err2
		}
		return ret, nil
	}

	// Error if the key is not found, and we don't have permission to create one.
	if !create {
		return nil, err
	}

	// Create a new sequence
	seq, seqerr := t.db.GetSequence(dbValue, 10)
	if seqerr != nil {
		return nil, seqerr
	}
	defer seq.Release()
	// Get the next available ID in sequence
	key, err := seq.Next()
	if err != nil {
		return nil, err
	}

	// Store the new key and value
	return store(txn, key, v, hash)
}

func store(txn *badger.Txn, key uint64, value []byte, hash []byte) ([]byte, error) {
	k := make([]byte, 9)
	raw := make([]byte, 9)

	if len(value) > 40 {
		// If the value length > 40, generate a sha256
		binary.LittleEndian.PutUint64(k[1:9], key+hashKeySpace)
		k[0] = dbHashKey[0]
		raw = hash
	} else {
		// For smaller values just use the value
		binary.LittleEndian.PutUint64(k[1:9], key)
		k[0] = dbKey[0]
		raw = value
	}
	// Set the combinations into the database
	err := txn.Set(k, value)
	if err != nil {
		return nil, err
	}
	err = txn.Set(raw, k)
	if err != nil {
		return nil, err
	}
	return k, nil
}

// Materialize transforms keys into values
func (t *Triplestore) Materialize(keys [][]byte) []interface{} {
	// Make an output array to hold the keys
	res := make([]interface{}, len(keys))
	// Open a read only view
	err := t.db.View(func(txn *badger.Txn) error {
		// Loop through the keys
		for j, i := range keys {
			// Get the entry
			value, err := txn.Get(i)
			if err != nil {
				return err
			}
			getValue := func(val []byte) error {
				rval := unmarshal(val[1:])
				res[j] = rval
				return nil
			}
			value.Value(func(val []byte) error {
				if val[0] == dbHash[0] {
					// If the value starts with the dbHash identifier, jump and get the actual value
					value2, _ := txn.Get(val)
					return value2.Value(getValue)
				}
				return getValue(val)
			})
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return res
}
