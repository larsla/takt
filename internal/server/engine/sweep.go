package engine

import (
	"context"
	"strings"
)

// Sweep finalizes runs whose last commit was interrupted, for example by a
// server crash between writing state and appending history. It returns how
// many runs it finalized.
func (e *Engine) Sweep(ctx context.Context) (int, error) {
	lister, err := e.execs.ListKeys(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for k := range lister.Keys() {
		if strings.HasSuffix(k, ".current") {
			continue
		}
		s, _, err := e.get(ctx, k)
		if err != nil || (len(s.Pending) == 0 && len(s.Outbox) == 0) {
			continue
		}
		if _, _, err := e.load(ctx, k); err != nil {
			e.log.Warn("sweep finalize", "key", k, "err", err)
			continue
		}
		n++
	}
	return n, nil
}
