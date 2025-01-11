package memdb

import (
	"testing"
)

// Benchmark for insert operations
func BenchmarkTxnInsert(b *testing.B) {
	// Create a new memdb instance
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	// Benchmark the insert operation
	b.ResetTimer()
	txn := db.Txn(true)
	for i := 0; i < b.N; i++ {
		obj := testObjWithId(i)
		// Start a write transaction
		// Insert an object
		err := txn.Insert("main", obj)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
		// Commit the transaction
	}
	txn.Commit()
}

func BenchmarkTxnBulkInsert(b *testing.B) {
	// Create a new memdb instance
	db, err := NewMemDB(testValidSchema())
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	// Benchmark the insert operation
	b.ResetTimer()
	objs := make([]interface{}, 0)
	for i := 0; i < b.N; i++ {
		obj := testObjWithId(i)
		objs = append(objs, obj)
	}
	// Start a write transaction
	txn := db.Txn(true)
	// Insert an object
	err = txn.BulkInsert("main", objs)
	if err != nil {
		b.Fatalf("insert failed: %v", err)
	}
	// Commit the transaction
	txn.Commit()
}
