package aeds

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

// Sequence represents a sequence of int64 values which are assigned
// atomically and sequentially.  The current value of a sequence is stored
// in datastore within the kind "sequences".
type Sequence struct {
	// Name is a unique name for this sequence
	Name string

	// Minimum specifies the smallest value the sequence is allowed to hold.
	Minimum int64

	// Maximum specifies the largest value the sequence is allowed to hold.
	// Consider using math.MaxInt64
	Maximum int64

	// Start specifies the first value of this sequence.  It's used when fetching
	// the next value on a sequence which hasn't been stored in the datastore yet.
	Start int64

	// Increment specifies which value is added to the current sequence value
	// to obtain the next sequence value.  Both positive and negative numbers
	// are allowed.
	Increment int64
}

type sequenceValue struct {
	Name  string `datastore:",noindex"`
	Value int64  `datastore:",noindex"`
}

func (self Sequence) key(c context.Context) *datastore.Key {
	return datastore.NewKey(c, "sequences", self.Name, 0, nil)
}

// Next fetches the next value in the sequence and stores it as the current
// value in datastore.  This method should only be called from inside a
// datastore transaction.
func (self Sequence) Next(c context.Context) int64 {
	n, ok := self.MaybeCurrent(c)
	if ok {
		n = n + self.Increment
	} else {
		n = self.Start
	}

	// write new value to datastore
	key := self.key(c)
	x := sequenceValue{Name: self.Name, Value: n}
	_, err := datastore.Put(c, key, &x)
	if err != nil {
		panic(err)
	}

	return n
}

// MaybeCurrent returns the current value of the sequence or false if the
// sequence has no value yet.
func (self Sequence) MaybeCurrent(c context.Context) (int64, bool) {
	x := new(sequenceValue)
	err := datastore.Get(c, self.key(c), x)
	if err == datastore.ErrNoSuchEntity {
		return 0, false
	}

	return x.Value, true
}

// Current returns the current value of the sequence or panics if the sequence
// has no value yet.
func (self Sequence) Current(c context.Context) int64 {
	n, ok := self.MaybeCurrent(c)
	if !ok {
		log.Panicf("Sequence %s has no current value", self.Name)
	}
	return n
}
