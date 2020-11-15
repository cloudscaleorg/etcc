package events

import (
	"context"

	"github.com/rs/zerolog/log"
	etcd "go.etcd.io/etcd/clientv3"
)

func buffer(ctx context.Context, l *Listener) State {
	// create a new fence map here.
	// if etcd compaction or error happens during "listening" we trash the old map
	// and create a new smaller one.
	l.FenceMap = NewFencer()
	wC := l.Client.Watcher.Watch(ctx, l.Prefix, etcd.WithPrefix(), etcd.WithPrevKV())
	l.eC = Stream(ctx, wC)
	l.logger.Debug().Msg("now buffering events")
	return Snapshot
}

// Stream demultiplexes events from an etcd.WatchResponse and delivers them to the returned channel.
// provides channel semantics, such as ranging, to the caller.
// remember to cancel context to not leak go routines
func Stream(ctx context.Context, wC etcd.WatchChan) <-chan *etcd.Event {
	eC := make(chan *etcd.Event, 1024)

	go func(ctx context.Context, ec chan *etcd.Event) {
		// this unblocks any callers ranging on ec
		defer close(ec)

		// etcd client will close this channel if error occurs
		for wResp := range wC {
			if err := ctx.Err(); err != nil {
				log.Info().Str("component", "Stream").Msgf("stream ctx canceled. returning: %v", err)
				return
			}

			if wResp.Canceled {
				log.Info().Str("component", "Stream").Msgf("watch channel error encountered. returning: %v", wResp.Err())
				return
			}

			for _, event := range wResp.Events {
				eC <- event
			}
		}
	}(ctx, eC)

	return eC
}
