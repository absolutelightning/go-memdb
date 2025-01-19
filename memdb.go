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

// getTableData is used to return the radix tree keys and values for a table and index
// for all the objects provided. Mostly logic is derived from the transaction's insert method.
func getTableData(db *MemDB, tName, idxName string, objs []interface{}) ([][]byte, []interface{}, error) {
	tableSchema, ok := db.schema.Tables[tName]
	if !ok {
		return nil, nil, fmt.Errorf("table not found: %s", tName)
	}
	idSchema, ok := tableSchema.Indexes[id]
	if !ok {
		return nil, nil, fmt.Errorf("primary index '%s' not found", id)
	}
	indexSchema, ok := tableSchema.Indexes[idxName]
	if !ok {
		return nil, nil, fmt.Errorf("index '%s' not found in table '%s'", idxName, tName)
	}

	idIndexer, ok := idSchema.Indexer.(SingleIndexer)
	if !ok {
		return nil, nil, fmt.Errorf("primary index '%s' must be SingleIndexer", id)
	}

	// Pre-allocate with len(objs) capacity
	radixKeys := make([][]byte, 0, len(objs))
	radixValues := make([]interface{}, 0, len(objs))

	for _, obj := range objs {
		// Get primary ID
		ok1, idVal, err := idIndexer.FromObject(obj)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build primary index: %v", err)
		}
		if !ok1 {
			return nil, nil, fmt.Errorf("object missing primary index")
		}

		// Build index value(s)
		var (
			okVal bool
			vals  [][]byte
		)

		switch indexer := indexSchema.Indexer.(type) {
		case SingleIndexer:
			var val []byte
			okVal, val, err = indexer.FromObject(obj)
			vals = [][]byte{val}
		case MultiIndexer:
			okVal, vals, err = indexer.FromObject(obj)
		default:
			return nil, nil, fmt.Errorf("unknown indexer type for '%s'", idxName)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("failed to build index '%s': %v", idxName, err)
		}

		// For non-unique indexes, append the primary key
		if okVal && !indexSchema.Unique {
			for i := range vals {
				vals[i] = append(vals[i], idVal...)
			}
		}

		// If missing is allowed, skip object entirely
		if !okVal {
			if indexSchema.AllowMissing {
				continue
			}
			return nil, nil, fmt.Errorf("missing value for index '%s'", idxName)
		}

		// Collect
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
	// A struct to hold the results for each (table, index)
	type indexResult struct {
		path  string
		index *iradix.Tree
		err   error
	}

	results := make(chan indexResult, 16)
	var wg sync.WaitGroup

	// Spawn a goroutine per (table, index) to build the partial index
	for tName, objects := range tableData {
		// Grab the table schema once
		schema, ok := db.schema.Tables[tName]
		if !ok {
			// If a table is missing, immediately fail.
			// You might prefer to push this into the channel
			// so all other indexing can complete if desired.
			return fmt.Errorf("table not found: %s", tName)
		}

		for iName := range schema.Indexes {
			wg.Add(1)

			go func(tableName, indexName string, objs []interface{}) {
				defer wg.Done()

				keys, vals, err := getTableData(db, tableName, indexName, objs)
				if err != nil {
					results <- indexResult{"", nil, err}
					return
				}

				idx := iradix.NewWithData(keys, vals)
				path := indexPath(tableName, indexName)
				results <- indexResult{string(path), idx, nil}
			}(tName, iName, objects)
		}
	}

	// Close channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Insert each partial index into the root, serially
	root := db.getRoot()
	for res := range results {
		if res.err != nil {
			return res.err
		}
		root, _, _ = root.Insert([]byte(res.path), res.index)
	}

	db.root = unsafe.Pointer(root)
	return nil
}

// indexPath returns the path from the root to the given table index
func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
