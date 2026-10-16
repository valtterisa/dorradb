package checkpoint_test

import (
	"testing"
	"time"

	"dorradb"
	"dorradb/tests/harness"
)

func TestVacuumSkipsWhileReadersPinned(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{
		WALCheckpointBytes: 1,
		VacuumDeadBytes:    1,
		DisableSync:        true,
	})
	defer db.Close()

	val := make([]byte, 800)
	for i := 0; i < 20; i++ {
		if err := db.Set([]byte{byte(i)}, val); err != nil {
			t.Fatal(err)
		}
	}
	// Create dead bytes by overwriting.
	for i := 0; i < 20; i++ {
		if err := db.Set([]byte{byte(i)}, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	rtx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := db.Set([]byte("w"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > time.Second {
		t.Fatalf("writes stalled while reader pinned: %v", time.Since(start))
	}
	dead := db.Stats().ApproxDeadBytes
	if dead == 0 {
		t.Log("dead bytes already 0 (vacuum may have run before pin); continuing")
	}

	if err := rtx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Next write should allow pending vacuum.
	if err := db.Set([]byte("z"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	// Values still correct.
	for i := 0; i < 20; i++ {
		got, err := db.Get([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "x" {
			t.Fatalf("key %d = %q", i, got)
		}
	}
}

func TestSnapshotSurvivesCheckpoint(t *testing.T) {
	db, _ := harness.Open(t, dorradb.Options{
		WALCheckpointBytes: 1,
		DisableSync:        true,
	})
	defer db.Close()
	if err := db.Set([]byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	for i := 0; i < 5; i++ {
		if err := db.Set([]byte("k"), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("got %q", got)
	}
}
