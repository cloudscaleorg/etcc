package events

import (
	"github.com/rs/zerolog/log"
	etcd "go.etcd.io/etcd/mvcc/mvccpb"
)

// FenceMap holds the current revision number for a particular key in etcd.
type FenceMap map[string]int64

// NewFencer is a constructor for a Fencer.
func NewFencer() FenceMap {
	var f FenceMap
	f = make(map[string]int64)
	return f
}

// Fence indicates to the caller whether they should reduce the current event or discard.
// true indicates you should fence (discard) the event. false indicates you should not.
func (m FenceMap) Fence(kv *etcd.KeyValue) bool {
	id := string(kv.Key)
	// ModRevision will always increases even when key deleted
	inRev := kv.ModRevision
	curRev, ok := m[id]
	switch {
	case inRev <= curRev:
		// stale rev, fence off.
		log.Debug().Str("component", "FenceMap").Msgf("fenced: %v revision: %v stale", id, inRev)
		return true
	case !ok:
		log.Debug().Str("component", "FenceMap").Msgf("new: %v revision: %v", id, inRev)
	default:
		log.Debug().Str("component", "FenceMap").Msgf("updated: %v revision %v", id, inRev)
	}
	m[id] = inRev
	return false
}
