package aeds

import (
	"bytes"
	"encoding/gob"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/memcache"
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
func Key(c context.Context, e Entity) *datastore.Key {
	return datastore.NewKey(c, e.Kind(), e.StringId(), 0, nil)
}

// Get retrieves an entity directly from the datastore, skipping all
// caches.  Most code should use FromId but Get can be helpful inside
// datastore transactions where caching would interfere.
//
// Get automatically calls IdempotentReset, if applicable, to handle
// retrying transactions.
func Get(c context.Context, e Entity) error {
	if x, ok := e.(NeedsIdempotentReset); ok {
		x.IdempotentReset()
	}

	lookupKey := Key(c, e)
	err := datastore.Get(c, lookupKey, e)
	if err == nil || IsErrFieldMismatch(err) {
		if x, ok := e.(HasGetHook); ok {
			x.HookAfterGet()
		}
		return nil
	}
	return err
}

// Put stores an entity in the datastore.
func Put(c context.Context, e Entity) (*datastore.Key, error) {
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
	err = ClearCache(c, e)
	if err != nil {
		log.Errorf(c, "aeds.Put ClearCache error: %s", err)
	}

	return key, nil
}

// PutMulti stores many entities in the datastore.
func PutMulti(c context.Context, es []Entity) ([]*datastore.Key, error) {
	keys := make([]*datastore.Key, 0, len(es))

	// prepare for PutMulti
	for _, e := range es {
		if x, ok := e.(HasPutHook); ok {
			x.HookBeforePut()
		}
		keys = append(keys, Key(c, e))
	}

	keys, err := datastore.PutMulti(c, keys, es)
	if err != nil {
		return nil, err
	}

	// delete from memcache?
	for _, e := range es {
		err = ClearCache(c, e)
		if err != nil {
			log.Errorf(c, "aeds.Put ClearCache error: %s", err)
		}
	}

	return keys, nil
}

// ClearCache explicitly clears any memcache entries associated with this
// entity. One doesn't usually call this function directly.  Rather, it's called
// implicitly when other aeds functions know the cache should be cleared.
func ClearCache(c context.Context, e Entity) error {
	// nothing to do for uncacheable entities
	if !canBeCached(e) {
		return nil
	}

	err := memcache.Delete(c, Key(c, e).String())
	switch err {
	case nil:
	case memcache.ErrCacheMiss:
	default:
		return err
	}

	return nil
}

// Delete removes an entity from the datastore.
func Delete(c context.Context, e Entity) error {
	lookupKey := Key(c, e)

	// should the entity be removed from memcache too?
	err := ClearCache(c, e)
	if err != nil {
		return err
	}

	return datastore.Delete(c, lookupKey)
}

// FromId fetches an entity based on its ID.  The given entity
// should have enough data to calculate the entity's key.  On
// success, the entity is modified in place with all data from
// the datastore.
// Field mismatch errors are ignored.
func FromId(c context.Context, e Entity) (Entity, error) {
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
//
// You should not perform any datastore operations inside f.  By design, it
// doesn't have access to the transactional context used internally.  Other
// datastore changes will happen, even if the transaction fails to commit.
func Modify(c context.Context, e Entity, f func(Entity) error) error {
	key := Key(c, e)

	err := datastore.RunInTransaction(c, func(c context.Context) error {
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
	err = ClearCache(c, e)
	if err != nil {
		return err
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
