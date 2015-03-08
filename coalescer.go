package coalescer

import (
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

var (
	// ErrRollback is returned from Update() when a future transaction in the
	// same coalescing group returns an error and rolls back the transaction.
	ErrRollback = errors.New("rollback")

	// ErrInvalidLimit is returned when the limit passed to New() is
	// a non-positive integer.
	ErrInvalidLimit = errors.New("invalid coalescer limit")

	// ErrInvalidInterval is returned when the interval passed to New() is
	// a non-positive duration.
	ErrInvalidInterval = errors.New("invalid coalescer interval")
)

// Coalescer automatically groups together Bolt write transactions and flushes
// them together as a single transaction. This approach is useful for increasing
// write throughput. However, because all transactions are grouped together,
// rolling back one transaction will roll back all of them.
type Coalescer struct {
	db       *bolt.DB
	limit    int
	interval time.Duration

	tx       *bolt.Tx
	handlers chan *handler
	force    chan bool
	count    chan bool
}

// New returns a new transaction Coalescer for a Bolt database.
// The coalescer will automatically flush when the number of transactions
// reaches the limit or after the interval has passed. If limit or interval
// is zero then those parameters are ignored.
func New(db *bolt.DB, limit int, interval time.Duration) (*Coalescer, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	if interval <= 0 {
		return nil, ErrInvalidInterval
	}

	// Instantiate object.
	c := &Coalescer{
		db:       db,
		limit:    limit,
		interval: interval,
		handlers: make(chan *handler, limit),
		force:    make(chan bool),
		count:    make(chan bool),
	}

	// Start a separate goroutine to periodically flush the updates.
	go c.flusher()

	// Start a separate goroutine to track when to send a force.
	go c.counter()

	return c, nil
}

// Update executes a function in the context of a write transaction.
func (c *Coalescer) Update(fn func(tx *bolt.Tx) error) error {
	c.count <- true
	h := &handler{fn, make(chan error)}
	c.handlers <- h
	return <-h.ch
}

// Make counter thread safe, by having a single goroutine touch it.
func (c *Coalescer) counter() {
	count := 0
	for {
		<-c.count
		count += 1
		if count >= c.limit {
			c.force <- true
			count = 0
		}

	}
}

// flusher continually runs in the background and flushes transactions at
// given intervals and limits.
func (c *Coalescer) flusher() {
	for {
		c.flush()
	}
}

// flush is called by flusher() to manage a single flush.
func (c *Coalescer) flush() {
	// Wait for a given interval or until the flush is forced because
	// the number of handlers has exceeded the limit.
	select {
	case <-time.After(c.interval):
	case <-c.force:
	}

	// Ignore flush if we have no queued updates.
	if len(c.handlers) == 0 {
		return
	}

	// Iterate over all the handlers
	var handlers []*handler
	err := c.db.Update(func(tx *bolt.Tx) error {
		for {
			select {
			case h := <-c.handlers:
				// Excute handler and return it's error if one occurs.
				// This will cause a rollback to all previous handlers in
				// this coalesce group.
				if err := h.fn(tx); err != nil {
					h.ch <- err
					return ErrRollback
				}

				// Track the handler so we can return a rollback if a future
				// handler returns an error.
				handlers = append(handlers, h)
			default:
				return nil
			}
		}
	})

	// Notify all handlers of an error, if one occurred.
	// If a handler causes an error then that specific handler will return its
	// error but all previous handlers will receive a generic "rollback" error.
	for _, h := range handlers {
		h.ch <- err
	}
}

// handler represents a handler for update functions and a channel to receive
// any resulting errors that occur during a coalesced update.
type handler struct {
	fn func(*bolt.Tx) error
	ch chan error
}
