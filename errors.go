package aeds

import (
	"strings"

	"google.golang.org/appengine/datastore"
)

// Returns true if the given error is a datastore deadline exceeded error
func IsDeadlineExceeded(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadline exceeded") ||
		strings.Contains(msg, "operation timed out") ||
		strings.Contains(msg, "TIMEOUT") ||
		strings.Contains(msg, "query has expired")
}

// IsErrFieldMismatch returns whether err is a datastore.ErrFieldMismatch.
// This error happens when loading an entity from the datastore into a
// struct which doesn't have all the necessary fields.
//
// It happens routinely when struct definitions change by removing a field.
func IsErrFieldMismatch(err error) bool {
	_, ok := err.(*datastore.ErrFieldMismatch)
	return ok
}
