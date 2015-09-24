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

// NeedsIdempotentReset is implemented by any Entity that needs to reset its
// fields to zero when performing datastore read operations that are intended to
// be idempotent. For example, a datastore read into a slice property is not
// idempotent (it just appends property values to the slice).  In that case,
// this method might manually reset the slice length to 0 so that append leaves
// only the desired data.
type NeedsIdempotentReset interface {
	IdempotentReset()
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

// Modify atomically executes a read, modify, write operation on a single
// entity.  It should be used any time the results of a datastore read influence
// the contents of a datastore write.  Before executing f, the contents of e
// will be overwritten with the latest data available from the datastore.
//
// f should return an error value if something goes wrong with the modification.
// Modify returns that error value.
//
// As always, hooks defined by HookAfterGet() and HookBeforePut() are
// automatically executed at the appropriate time.  Be sure to define
// IdempotentReset() if your entity has any slice properties.
func Modify(c appengine.Context, e Entity, f func(Entity) error) error {
	key := Key(c, e)

	err := datastore.RunInTransaction(c, func(c appengine.Context) error {
		// reset slice fields (inside the transaction so it's retried)
		if x, ok := e.(NeedsIdempotentReset); ok {
			x.IdempotentReset()
		}

		// fetch most recent entity from datastore
		err := datastore.Get(c, key, e)
		if err == nil || IsErrFieldMismatch(err) {
			if x, ok := e.(HasGetHook); ok {
				x.HookAfterGet()
			}
		} else {
			return err
		}

		// perform the modifications
		err = f(e)
		if err != nil {
			return err
		}

		// write entity to datastore
		if x, ok := e.(HasPutHook); ok {
			x.HookBeforePut()
		}
		_, err = datastore.Put(c, key, e)
		return err
	}, nil)

	// did the transaction succeed?
	if err != nil {
		return err
	}

	// delete cache entry (See Note_1)
	if canBeCached(e) {
		err = memcache.Delete(c, key.String())
		switch err {
		case nil:
		case memcache.ErrCacheMiss:
		default:
			return err
		}
	}

	return nil
}

// Note_1
//
// Memcache operations are not transactional.  All combinations of commit
// and delete-from-cache leave some window of time during which the cache is
// stale.  The best we can do is minimize the size of this window.
//
// If we delete cache before our transaction, someone else might read a value
// and populate the cache just before our transaction commits. That leaves a
// permanent window of stale cache data. If we delete cache inside our
// transaction, we end have the same problem.
//
// By deleting cache right after we commit, there's a small window of time
// between commit and delete when someone might read and populate the cache with
// stale data.  Very soon afterwards, we delete the cache.  The window of stale
// date is on the order of 10 ms.  That's the best combination available to us.

func canBeCached(e Entity) bool {
	x, ok := e.(CanBeCached)
	return ok && x.CacheTtl() > 0
}

// StructProperties returns a slice of properties indicating how this struct
// would be saved to the datastore if one were to call datastore.SaveStruct() on
// it. The struct is not actually written to the datastore.  src must be a
// struct pointer.
func StructProperties(src interface{}) (datastore.PropertyList, error) {
	propCh := make(chan datastore.Property)
	errCh := make(chan error)

	go func() {
		errCh <- datastore.SaveStruct(src, propCh)
	}()

	props := make(datastore.PropertyList, 0)
	for prop := range propCh {
		props = append(props, prop)
	}

	return props, <-errCh
}
