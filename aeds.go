package aeds

import (
	"bytes"
	"encoding/gob"
	"time"

	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

// interface for structures that can be stored in App Engine's datastore
type Entity interface {
	Kind() string
	StringId() string

	HookAfterGet()  // Calculate derived fields after fetching from datastore
	HookBeforePut() // Calculate derived fields before writing to datastore

	// CacheTtl indicates how long the entity should be cached in memcache.
	// Return zero to disable memcache.  If this method returns a non-zero
	// duration, the receiver should also implement the GobEncoder and
	// GobDecoder interfaces.
	CacheTtl() time.Duration
}

// Key returns a datastore key for this entity.
func Key(c appengine.Context, e Entity) *datastore.Key {
	return datastore.NewKey(c, e.Kind(), e.StringId(), 0, nil)
}

// Put stores an entity in the datastore.
func Put(c appengine.Context, e Entity) (*datastore.Key, error) {
	e.HookBeforePut()
	ttl := e.CacheTtl()

	// encode entity as a gob (before storing in datastore)
	var value bytes.Buffer
	if ttl > 0 {
		err := gob.NewEncoder(&value).Encode(e)
		if err != nil {
			return nil, err
		}
	}

	// store entity in the datastore
	lookupKey := Key(c, e)
	key, err := datastore.Put(c, lookupKey, e)
	if err != nil {
		return nil, err
	}

	// store entity in memcache too?
	if ttl > 0 {
		item := &memcache.Item{
			Key:        lookupKey.String(),
			Value:      value.Bytes(),
			Expiration: ttl,
		}
		err := memcache.Set(c, item)
		_ = err // ignore memcache errors
	}

	return key, nil
}

// Delete removes an entity from the datastore.
func Delete(c appengine.Context, e Entity) error {
	lookupKey := Key(c, e)

	// should the entity be removed from memcache too?
	if e.CacheTtl() > 0 {
		err := memcache.Delete(c, lookupKey.String())
		if err == memcache.ErrCacheMiss {
			// noop
		} else if err != nil {
			return err
		}
	}

	return datastore.Delete(c, lookupKey)
}

// FromId fetches an entity based on its ID.  The given entity
// should have enough data to calculate the entity's key.  On
// success, the entity is modified in place with all data from
// the datastore.
// Field mismatch errors are ignored.
func FromId(c appengine.Context, e Entity) (Entity, error) {
	lookupKey := Key(c, e)
	ttl := e.CacheTtl()

	// should we look in memcache too?
	cacheMiss := false
	if ttl > 0 {
		item, err := memcache.Get(c, lookupKey.String())
		if err == nil {
			buf := bytes.NewBuffer(item.Value)
			err := gob.NewDecoder(buf).Decode(e)
			e.HookAfterGet()
			return e, err
		}
		if err == memcache.ErrCacheMiss {
			cacheMiss = true
		}
		// ignore any memcache errors
	}

	// look in the datastore
	err := datastore.Get(c, lookupKey, e)
	if err == nil {
		e.HookAfterGet()

		// should we update memcache?
		if cacheMiss && ttl > 0 {
			e.HookBeforePut()

			// encode
			var value bytes.Buffer
			err := gob.NewEncoder(&value).Encode(e)
			if err != nil {
				return nil, err
			}

			// store
			item := &memcache.Item{
				Key:        lookupKey.String(),
				Value:      value.Bytes(),
				Expiration: ttl,
			}
			err = memcache.Set(c, item)
			_ = err // ignore memcache errors
		}

		return e, nil
	}
	if IsErrFieldMismatch(err) {
		e.HookAfterGet()
		return e, nil
	}
	return nil, err // unknown datastore error
}
