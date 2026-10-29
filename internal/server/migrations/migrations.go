// Package migrations provisions the JetStream streams, KV buckets and object
// stores takt needs. Each embedded file in files/ is one numbered migration.
// Applied migrations are recorded in the takt_meta KV bucket.
package migrations

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// MetaBucket holds takt's own bookkeeping, including the schema version.
const MetaBucket = "takt_meta"

const versionKey = "schema_version"

//go:embed files/*.json
var embedded embed.FS

// Migration is one provisioning step.
type Migration struct {
	Version      int                           `json:"-"`
	Name         string                        `json:"-"`
	Description  string                        `json:"description"`
	Streams      []jetstream.StreamConfig      `json:"streams"`
	KV           []jetstream.KeyValueConfig    `json:"kv"`
	ObjectStores []jetstream.ObjectStoreConfig `json:"object_stores"`
}

// Options tunes how migrations are applied.
type Options struct {
	// Replicas overrides the replica count of every resource. 0 keeps the
	// value from the migration file (which defaults to 1).
	Replicas int
}

// Load returns the embedded migrations sorted by version.
func Load() ([]Migration, error) {
	return load(embedded, "files")
}

func load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must look like 0001_name.json", e.Name())
		}
		v, err := strconv.Atoi(prefix)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %q: bad version prefix", e.Name())
		}
		if other, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", other, e.Name(), v)
		}
		seen[v] = e.Name()

		data, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var m Migration
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("migration %q: %w", e.Name(), err)
		}
		m.Version = v
		m.Name = e.Name()
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Apply runs every embedded migration newer than the recorded schema version
// and returns the resulting version.
func Apply(ctx context.Context, js jetstream.JetStream, opts Options) (int, error) {
	ms, err := Load()
	if err != nil {
		return 0, err
	}
	return apply(ctx, js, ms, opts)
}

func apply(ctx context.Context, js jetstream.JetStream, ms []Migration, opts Options) (int, error) {
	meta, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      MetaBucket,
		Description: "takt bookkeeping",
		History:     1,
		Storage:     jetstream.FileStorage,
		Replicas:    opts.Replicas,
	})
	if err != nil {
		return 0, fmt.Errorf("create %s: %w", MetaBucket, err)
	}

	current, rev, err := readVersion(ctx, meta)
	if err != nil {
		return 0, err
	}
	for _, m := range ms {
		if m.Version <= current {
			continue
		}
		if err := applyOne(ctx, js, m, opts); err != nil {
			return current, fmt.Errorf("migration %s: %w", m.Name, err)
		}
		// Compare-and-set so two servers starting at once can't both claim
		// the same step. Resources are created idempotently, so losing the
		// race is harmless: re-read and carry on.
		val := []byte(strconv.Itoa(m.Version))
		if rev == 0 {
			rev, err = meta.Create(ctx, versionKey, val)
		} else {
			rev, err = meta.Update(ctx, versionKey, val, rev)
		}
		if err != nil {
			if !errors.Is(err, jetstream.ErrKeyExists) && !isWrongSequence(err) {
				return current, fmt.Errorf("record version %d: %w", m.Version, err)
			}
			if current, rev, err = readVersion(ctx, meta); err != nil {
				return 0, err
			}
			continue
		}
		current = m.Version
	}
	return current, nil
}

func applyOne(ctx context.Context, js jetstream.JetStream, m Migration, opts Options) error {
	for _, cfg := range m.Streams {
		if opts.Replicas > 0 {
			cfg.Replicas = opts.Replicas
		}
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", cfg.Name, err)
		}
	}
	for _, cfg := range m.KV {
		if opts.Replicas > 0 {
			cfg.Replicas = opts.Replicas
		}
		if _, err := js.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("kv %s: %w", cfg.Bucket, err)
		}
	}
	for _, cfg := range m.ObjectStores {
		if opts.Replicas > 0 {
			cfg.Replicas = opts.Replicas
		}
		if _, err := js.CreateOrUpdateObjectStore(ctx, cfg); err != nil {
			return fmt.Errorf("object store %s: %w", cfg.Bucket, err)
		}
	}
	return nil
}

// Version returns the schema version recorded in NATS, or 0 if none.
func Version(ctx context.Context, js jetstream.JetStream) (int, error) {
	meta, err := js.KeyValue(ctx, MetaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, _, err := readVersion(ctx, meta)
	return v, err
}

func readVersion(ctx context.Context, meta jetstream.KeyValue) (int, uint64, error) {
	e, err := meta.Get(ctx, versionKey)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read schema version: %w", err)
	}
	v, err := strconv.Atoi(string(e.Value()))
	if err != nil {
		return 0, 0, fmt.Errorf("schema version %q: %w", e.Value(), err)
	}
	return v, e.Revision(), nil
}

func isWrongSequence(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
