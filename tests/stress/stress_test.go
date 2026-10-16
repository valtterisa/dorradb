package stress_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dorradb"
	"dorradb/tests/harness"
)

func TestConcurrentReadersTinyCache(t *testing.T) {
	if testing.Short() {
		t.Skip("stress")
	}
	// Small cache forces eviction under concurrent readers; keep frames > readers
	// so Get does not hit ErrNoFreeFrame when every frame is briefly pinned.
	db, _ := harness.Open(t, dorradb.Options{
		CachePages:      16,
		DisableSync:     true,
		VacuumDeadBytes: -1,
	})
	defer db.Close()

	const nKeys = 128
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := make([]byte, 256)
		copy(v, k)
		if err := db.Set([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			k := fmt.Sprintf("k%04d", i%nKeys)
			v := make([]byte, 4096)
			copy(v, []byte(fmt.Sprintf("%s-%d", k, i)))
			if err := db.Set([]byte(k), v); err != nil {
				errCh <- err
				return
			}
			i++
		}
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("k%04d", (id+i)%nKeys)
				if _, err := db.Get([]byte(k)); err != nil {
					errCh <- err
					return
				}
				i++
			}
		}(r)
	}

	time.Sleep(500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
