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
}

// HasGetHook is implemented by any Entity that wants to execute
// specific code after fetching the raw entity from datastore.
// This is often used to calculate derived fields.
type HasGetHook interface {
	HookAfterGet()
}

// HasPutHook is implemented by any Entity that wants to execute
// specific code before writing the raw entity to datastore.
// This is often used to calculate derived fields.
type HasPutHook interface {
	HookBeforePut()
}

// CanBeCached is implemented by any Entity that wants to
// have its values stored in memcache to improve read performance.
type CanBeCached interface {
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
	if x, ok := e.(HasPutHook); ok {
		x.HookBeforePut()
	}

	// store entity in the datastore
	lookupKey := Key(c, e)
	key, err := datastore.Put(c, lookupKey, e)
	if err != nil {
		return nil, err
	}

	// delete from memcache?
	if canBeCached(e) {
		err := memcache.Delete(c, lookupKey.String())
		switch err {
		case nil:
		case memcache.ErrCacheMiss:
		default:
			c.Errorf("aeds.Put memcache.Delete error: %s", err)
		}
	}

	return key, nil
}

// Delete removes an entity from the datastore.
func Delete(c appengine.Context, e Entity) error {
	lookupKey := Key(c, e)

	// should the entity be removed from memcache too?
	if canBeCached(e) {
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
	var ttl time.Duration
	if x, ok := e.(CanBeCached); ok {
		ttl = x.CacheTtl()
	}

	// should we look in memcache too?
	cacheMiss := false
	if ttl > 0 {
		item, err := memcache.Get(c, lookupKey.String())
		if err == nil {
			buf := bytes.NewBuffer(item.Value)
			err := gob.NewDecoder(buf).Decode(e)
			if x, ok := e.(HasGetHook); ok {
				x.HookAfterGet()
			}
			return e, err
		}
		if err == memcache.ErrCacheMiss {
			cacheMiss = true
		}
		// ignore any memcache errors
	}

	// look in the datastore
	err := datastore.Get(c, lookupKey, e)
	if err == nil || IsErrFieldMismatch(err) {
		if x, ok := e.(HasGetHook); ok {
			x.HookAfterGet()
		}

		// should we update memcache?
		if cacheMiss && ttl > 0 {
			if x, ok := e.(HasPutHook); ok {
				x.HookBeforePut()
			}

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
	return nil, err // unknown datastore error
}

func canBeCached(e Entity) bool {
	x, ok := e.(CanBeCached)
	return ok && x.CacheTtl() > 0
}
