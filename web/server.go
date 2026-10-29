// Package web serves the takt web UI.
package web

import (
	"context"
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/internal/server/migrations"
	"github.com/larsla/takt/web/views"
)

//go:embed static
var staticFiles embed.FS

// Options configures the UI handler.
type Options struct {
	NC     *nats.Conn
	JS     jetstream.JetStream
	Logger *slog.Logger
}

// UI is the HTTP handler for the web UI.
type UI struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	log    *slog.Logger
	router *mux.Router
}

// New builds the UI handler.
func New(opts Options) *UI {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	u := &UI{nc: opts.NC, js: opts.JS, log: opts.Logger, router: mux.NewRouter()}

	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// The embed directive guarantees the directory exists.
		panic(err)
	}
	u.router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServerFS(static)))
	u.router.HandleFunc("/healthz", u.healthz).Methods(http.MethodGet)
	u.router.HandleFunc("/", u.overview).Methods(http.MethodGet)
	u.router.HandleFunc("/partials/overview", u.overviewPartial).Methods(http.MethodGet)
	return u
}

func (u *UI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u.router.ServeHTTP(w, r)
}

func (u *UI) healthz(w http.ResponseWriter, _ *http.Request) {
	if u.nc == nil || !u.nc.IsConnected() {
		http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (u *UI) overview(w http.ResponseWriter, r *http.Request) {
	o, err := u.loadOverview(r.Context())
	if err != nil {
		u.fail(w, "load overview", err)
		return
	}
	u.render(w, r, views.OverviewPage(o))
}

func (u *UI) overviewPartial(w http.ResponseWriter, r *http.Request) {
	o, err := u.loadOverview(r.Context())
	if err != nil {
		u.fail(w, "load overview", err)
		return
	}
	u.render(w, r, views.OverviewBody(o))
}

func (u *UI) loadOverview(ctx context.Context) (views.Overview, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	o := views.Overview{
		NATSURL:       u.nc.ConnectedUrlRedacted(),
		ServerVersion: u.nc.ConnectedServerVersion(),
	}
	v, err := migrations.Version(ctx, u.js)
	if err != nil {
		return o, err
	}
	o.SchemaVersion = v

	streams := u.js.ListStreams(ctx)
	for info := range streams.Info() {
		s := views.Store{
			Messages: info.State.Msgs,
			Bytes:    info.State.Bytes,
			Subjects: strings.Join(info.Config.Subjects, " "),
		}
		switch name := info.Config.Name; {
		case strings.HasPrefix(name, "KV_"):
			s.Name = strings.TrimPrefix(name, "KV_")
			o.KeyValues = append(o.KeyValues, s)
		case strings.HasPrefix(name, "OBJ_"):
			s.Name = strings.TrimPrefix(name, "OBJ_")
			o.ObjectStores = append(o.ObjectStores, s)
		default:
			s.Name = name
			o.Streams = append(o.Streams, s)
		}
	}
	if err := streams.Err(); err != nil {
		return o, err
	}
	for _, list := range [][]views.Store{o.Streams, o.KeyValues, o.ObjectStores} {
		sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	}
	return o, nil
}

func (u *UI) render(w http.ResponseWriter, r *http.Request, c templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.Render(r.Context(), w); err != nil {
		u.log.Error("render", "path", r.URL.Path, "err", err)
	}
}

func (u *UI) fail(w http.ResponseWriter, what string, err error) {
	u.log.Error(what, "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}
