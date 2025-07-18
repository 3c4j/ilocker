package ilocker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrHasLocked = errors.New("locked")
)

type meta struct {
	id        string
	mu        sync.RWMutex // the mutex to protect released and releaseAt
	releaseAt time.Time    // the time that lock will be released automatically
	released  bool         // whether the lock is released by UnLock
}

func NewMeta(id string, ttl time.Duration) ILocked {
	m := &meta{
		id:        id,
		releaseAt: time.Now().Add(ttl),
	}

	return m
}

func (m *meta) Locking(ctx context.Context) bool {
	return !m.IsReleased()
}

func (m *meta) UnLock(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = true
	return nil
}

func (m *meta) IsReleased() bool {
	if !m.mu.TryLock() { // if lock failed, then return false
		return false
	}
	defer m.mu.Unlock()
	return m.released || m.releaseAt.Before(time.Now())
}

func (m *meta) Refresh(ttl time.Duration) error {
	if !m.mu.TryLock() {
		return ErrHasLocked
	}
	defer m.mu.Unlock()
	m.releaseAt = time.Now().Add(ttl)
	m.released = false
	return nil
}

type MemoryLocker struct {
	locked           sync.Map // map[string]*meta
	mu               sync.Map // map[string]*sync.RWMutex
	cleanupIsRunning atomic.Bool
}

// NewMemoryLocker The locker is implemented in memory.
// The locker will auto delete lock in another goroutine,
// and the check interval is 10ms by default.
// Please don't call the callback function unless the application will be exited.
func NewMemoryLocker(interval time.Duration) (ILocker, func(), error) {
	if interval <= 0 {
		interval = 10 * time.Microsecond
	}
	l := &MemoryLocker{
		locked:           sync.Map{},
		mu:               sync.Map{},
		cleanupIsRunning: atomic.Bool{},
	}

	// start a goroutine to clean up expired locks.
	go l.cleanup(time.Tick(interval))

	return l, func() {
		l.cleanupIsRunning.Store(false)
	}, nil
}

func (l *MemoryLocker) Lock(ctx context.Context, id string, ttl time.Duration) (ILocked, error) {
	locked, ok := l.locked.Load(id)
	if ok {
		_locked := locked.(*meta)
		if _locked.Locking(ctx) {
			return nil, ErrHasLocked
		}

		// If the lock is released but not released by locker,
		// then try to extend the lock that update the releaseAt.
		if err := _locked.Refresh(ttl); err == nil {
			return _locked, nil
		}
	}

	// Use sync.RWMutex to lock the id
	mu, _ := l.mu.LoadOrStore(id, &sync.RWMutex{})
	_mu, _ := mu.(*sync.RWMutex)
	if !_mu.TryLock() {
		return nil, ErrHasLocked
	}

	// TODO: Which defer is executed first? Why?
	defer l.mu.Delete(id)
	defer _mu.Unlock()

	newLock := NewMeta(id, ttl)
	l.locked.Store(id, newLock)
	return newLock, nil
}

func (l *MemoryLocker) Locking(ctx context.Context, id string) bool {
	locked, ok := l.locked.Load(id)
	if !ok {
		return false
	}

	return locked.(*meta).Locking(ctx)
}

func (l *MemoryLocker) UnLock(ctx context.Context, id string) error {
	ld, ok := l.locked.Load(id)
	if !ok {
		return nil
	}
	_locked, ok := ld.(*meta)
	if !ok {
		return nil
	}

	return _locked.UnLock(ctx)
}

func (l *MemoryLocker) cleanup(c <-chan time.Time) {
	// ticker don't close the channel, so we need to check the running status.
	l.cleanupIsRunning.Store(true)
	for {
		if !l.cleanupIsRunning.Load() {
			return
		}

		select {
		case <-c:
			l.locked.Range(func(key, value any) bool {
				locked, ok := value.(*meta)
				if !ok || locked.IsReleased() {
					l.locked.Delete(key)
				}
				return true
			})
		}
	}
}
