// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

// Package memdb provides an in-memory database that supports transactions
// and MVCC.
package memdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

// MemDB is an in-memory database providing Atomicity, Consistency, and
// Isolation from ACID. MemDB doesn't provide Durability since it is an
// in-memory database.
//
// MemDB provides a table abstraction to store objects (rows) with multiple
// indexes based on inserted values. The database makes use of immutable radix
// trees to provide transactions and MVCC.
//
// Objects inserted into MemDB are not copied. It is **extremely important**
// that objects are not modified in-place after they are inserted since they
// are stored directly in MemDB. It remains unsafe to modify inserted objects
// even after they've been deleted from MemDB since there may still be older
// snapshots of the DB being read from other goroutines.
type MemDB struct {
	schema  *DBSchema
	root    unsafe.Pointer // *iradix.Tree underneath
	primary bool

	// There can only be a single writer at once
	writer sync.Mutex
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDB(schema *DBSchema) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}

	return db, nil
}

// DBSchema returns schema in use for introspection.
//
// The method is intended for *read-only* debugging use cases,
// returned schema should *never be modified in-place*.
func (db *MemDB) DBSchema() *DBSchema {
	return db.schema
}

// getRoot is used to do an atomic load of the root pointer
func (db *MemDB) getRoot() *iradix.Tree {
	root := (*iradix.Tree)(atomic.LoadPointer(&db.root))
	return root
}

// Txn is used to start a new transaction in either read or write mode.
// There can only be a single concurrent writer, but any number of readers.
func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
	}
	return txn
}

// Snapshot is used to capture a point-in-time snapshot  of the database that
// will not be affected by any write operations to the existing DB.
//
// If MemDB is storing reference-based values (pointers, maps, slices, etc.),
// the Snapshot will not deep copy those values. Therefore, it is still unsafe
// to modify any inserted values in either DB.
func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema:  db.schema,
		root:    unsafe.Pointer(db.getRoot()),
		primary: false,
	}
	return clone
}

// NewMemDB creates a new MemDB with the given schema.
func NewMemDBWithData(schema *DBSchema, data map[string][]interface{}) (*MemDB, error) {
	// Validate the schema
	if err := schema.Validate(); err != nil {
		return nil, err
	}

	// Create the MemDB
	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
	}
	if err := db.initializeWithObjects(data); err != nil {
		return nil, err
	}
	return db, nil
}

// initialize is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

func getTableData(db *MemDB, tName, index string, objs []interface{}) ([][]byte, []interface{}, error) {
	// Get the table schema
	tableSchema, ok := db.schema.Tables[tName]
	if !ok {
		panic("table not found")
	}

	idSchema := tableSchema.Indexes[id]
	indexSchema := tableSchema.Indexes[index]
	radixKeys := make([][]byte, 0)
	radixValues := make([]interface{}, 0)
	idIndexer := idSchema.Indexer.(SingleIndexer)
	for _, obj := range objs {
		// Get the primary ID of the object
		ok1, idVal, err1 := idIndexer.FromObject(obj)
		if err1 != nil {
			return nil, nil, fmt.Errorf("failed to build primary index: %v", err1)
		}
		if !ok1 {
			return nil, nil, fmt.Errorf("object missing primary index")
		}

		// Determine the new index value
		var (
			ok   bool
			err  error
			vals [][]byte
		)
		switch indexer := indexSchema.Indexer.(type) {
		case SingleIndexer:
			var val []byte
			ok, val, err = indexer.FromObject(obj)
			vals = [][]byte{val}
		case MultiIndexer:
			ok, vals, err = indexer.FromObject(obj)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build index '%s': %v", index, err)
		}

		// Handle non-unique index by computing a unique index.
		// This is done by appending the primary key which must
		// be unique anyways.
		if ok && !indexSchema.Unique {
			for i := range vals {
				vals[i] = append(vals[i], idVal...)
			}
		}

		// If there is no index value, either this is an error or an expected
		// case and we can skip updating
		if !ok {
			if indexSchema.AllowMissing {
				continue
			} else {
				return nil, nil, fmt.Errorf("missing value for index '%s'", index)
			}
		}

		for _, val := range vals {
			radixKeys = append(radixKeys, val)
			radixValues = append(radixValues, obj)
		}
	}

	return radixKeys, radixValues, nil
}

// initialize with data is used to setup the DB for use after creation. This should
// be called only once after allocating a MemDB.
func (db *MemDB) initializeWithObjects(tableData map[string][]interface{}) error {
	root := db.getRoot()
	for tName, _ := range tableData {
		for iName := range db.schema.Tables[tName].Indexes {
			keys, vals, err := getTableData(db, tName, iName, tableData[tName])
			if err != nil {
				return err
			}
			index := iradix.NewWithData(keys, vals)
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
