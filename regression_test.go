package dorradb_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"dorradb"
)

func TestRegressionHotKeyUpdates(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true})
	defer db.Close()
	v := bytes.Repeat([]byte("x"), 1000)
	for i := 0; i < 10; i++ {
		if err := db.Set([]byte("k"), v); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
}

func TestRegressionBigInlineSplit(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true})
	defer db.Close()
	v := bytes.Repeat([]byte("y"), 1900)
	for i := 0; i < 6; i++ {
		if err := db.Set([]byte{byte('a' + i)}, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	for i := 0; i < 6; i++ {
		if _, err := db.Get([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
	}
}

func TestRegressionConcurrentReadWrite(t *testing.T) {
	db, _ := dorradb.Open(filepath.Join(t.TempDir(), "a.dorradb"), dorradb.Options{DisableSync: true, CachePages: 8})
	defer db.Close()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < 50; i++ {
					_, _ = db.Get([]byte(fmt.Sprintf("k%04d", i)))
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%04d", i%500)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestRegressionCrashWithSmallCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.dorradb")
	db, _ := dorradb.Open(path, dorradb.Options{DisableSync: true, CachePages: 8})
	const n = 2000
	for i := 0; i < n; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate kill -9: copy files without Close.
	crash := filepath.Join(t.TempDir(), "c.dorradb")
	for _, suf := range []string{"", "-wal"} {
		b, err := os.ReadFile(path + suf)
		if err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(crash+suf, b, 0o644)
	}
	db.Close()
	db2, err := dorradb.Open(crash, dorradb.Options{DisableSync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("k%05d", i)))
		if err != nil || string(got) != fmt.Sprintf("v%d", i) {
			t.Fatalf("i=%d got %q err=%v", i, got, err)
		}
	}
}
