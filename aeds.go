package aeds

import (
	"appengine"
	"appengine/datastore"
)

import . "util"

// interface for structures that can be stored in App Engine's datastore
type Entity interface {
	Kind() string
	StringId() string
	HookBeforePut() // Calculate derived fields before writing to datastore
}

// Key returns a datastore key for this entity.
func Key(c appengine.Context, e Entity) *datastore.Key {
	return datastore.NewKey(c, e.Kind(), e.StringId(), 0, nil)
}

// Put stores an entity in the datastore.
func Put(c appengine.Context, e Entity) (*datastore.Key, error) {

	// store the event itself
	e.HookBeforePut()
	return datastore.Put(c, Key(c, e), e)
}

// Delete removes an entity from the datastore.
func Delete(c appengine.Context, e Entity) error {
	return datastore.Delete(c, Key(c, e))
}

// FromId fetches an entity based on its ID.  The given entity
// should have enough data to calculate the entity's key.
func FromId(c appengine.Context, e Entity) Entity {
	err := datastore.Get(c, Key(c, e), e)
	if err == nil {
		return e
	}
	if IsErrFieldMismatch(err) {
		return e
	}
	if err == datastore.ErrNoSuchEntity {
		return nil
	}
	panic(err) // unknown datastore error
}
