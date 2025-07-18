package ilocker

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestCleanupGoroutine(t *testing.T) {
	locker, fn, err := NewMemoryLocker(0)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_locker := locker.(*MemoryLocker)
	assert.True(t, _locker.cleanupIsRunning.Load(), "check goroutine is not running")
	fn()
	time.Sleep(time.Second)
	assert.False(t, _locker.cleanupIsRunning.Load(), "check goroutine is running")
}

func TestReleaseWithExpired(t *testing.T) {
	locker, fn, err := NewMemoryLocker(0)
	if err != nil {
		t.Fatal(err)
	}
	defer fn()

	ctx := context.TODO()
	locked, err := locker.Lock(ctx, "test", 3*time.Second)
	assert.Nil(t, err, "lock failed")
	assert.True(t, locked.Locking(ctx), "locked but locking is false")

	time.Sleep(4 * time.Second)

	assert.False(t, locked.Locking(ctx), "lock is expired but not release")
	assert.False(t, locker.Locking(ctx, "test"), "lock is expired but not release")
}

func TestLocking(t *testing.T) {
	locker, fn, err := NewMemoryLocker(0)
	if err != nil {
		t.Fatal(err)
	}
	defer fn()

	ctx := context.TODO()
	// Lock
	locked, err := locker.Lock(ctx, "test", 3*time.Second)
	assert.Nil(t, err, "lock failed at first time")
	assert.True(t, locked.Locking(ctx), "locked but locking is false at first time")

	// lock again
	_, err = locker.Lock(ctx, "test", 3*time.Second)
	assert.Equal(t, ErrHasLocked, err, "lock is exist, and lock again should be failed")

	// Release lock using 'ilocker.ILocked'
	err = locked.UnLock(ctx)
	assert.Nil(t, err, "release lock using 'locked' failed")

	// lock again
	locked, err = locker.Lock(ctx, "test", 3*time.Second)
	assert.Nil(t, err, "lock failed at second time")
	assert.True(t, locked.Locking(ctx), "locked but locking is false at second time")

	// Release lock using 'ilocker.ILocker'
	err = locker.UnLock(ctx, "test")
	assert.Nil(t, err, "release lock using 'locker' failed")

	// lock again
	locked, err = locker.Lock(ctx, "test", 3*time.Second)
	assert.Nil(t, err, "lock failed at third time")
	assert.True(t, locked.Locking(ctx), "locked but locking is false at third time")
}

func TestLocking_Async(t *testing.T) {
	locker, fn, err := NewMemoryLocker(0)
	if err != nil {
		t.Fatal(err)
	}
	defer fn()

	ctx := context.TODO()

	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		locked, err := locker.Lock(ctx, "test", 3*time.Second)
		assert.Nil(t, err, "lock failed")
		assert.True(t, locked.Locking(ctx), "locked but locking is false at first goroutine")
	}()

	go func() {
		defer wg.Done()
		time.Sleep(time.Second)
		_, err := locker.Lock(ctx, "test", 3*time.Second)
		assert.Equal(t, ErrHasLocked, err, "lock is exist in first goroutine, and lock again should be failed")
	}()

	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 4)
		_, err := locker.Lock(ctx, "test", 3*time.Second)
		assert.Nil(t, err, "lock is release with 3 seconds, and lock again should be success")
	}()

	wg.Wait()
}
