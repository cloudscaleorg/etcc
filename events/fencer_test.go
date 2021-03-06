package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/mvcc/mvccpb"
)

// TestFencer_Fence confirms older revisions will
// not be reduced
func TestFencer_Fence(t *testing.T) {
	var table = []struct {
		name   string
		key    string
		oldRev int64
		newRev int64
	}{
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(100),
			newRev: int64(100),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(200),
			newRev: int64(100),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(300),
			newRev: int64(100),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(400),
			newRev: int64(100),
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			fm := NewFencer()

			fm[tt.key] = tt.oldRev
			ok := fm.Fence(&etcd.KeyValue{
				Key:         []byte(tt.key),
				ModRevision: tt.newRev,
			})

			assert.True(t, ok)
		})
	}
}

// TestFencer_Update confirms newer revision
// will be reduced
func TestFencer_Update(t *testing.T) {
	var table = []struct {
		name   string
		key    string
		oldRev int64
		newRev int64
	}{
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(100),
			newRev: int64(101),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(100),
			newRev: int64(200),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(100),
			newRev: int64(300),
		},
		{
			name:   "key update",
			key:    "test-key",
			oldRev: int64(100),
			newRev: int64(400),
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			fm := NewFencer()

			fm[tt.key] = tt.oldRev
			ok := fm.Fence(&etcd.KeyValue{
				Key:         []byte(tt.key),
				ModRevision: tt.newRev,
			})

			assert.False(t, ok)
		})
	}
}
