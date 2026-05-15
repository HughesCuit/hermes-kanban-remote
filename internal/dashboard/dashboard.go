// Package dashboard provides the Web Dashboard for hermes-agent-cluster.
// It embeds the static HTML/CSS/JS files and serves them at /dashboard/.
package dashboard

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
)

//go:embed static/*
var staticFiles embed.FS

// Handler returns an http.Handler that serves the dashboard static files.
// The returned handler strips the "static" prefix so files are served from root.
// If the embedded filesystem is malformed, returns a 500 handler instead of panicking.
func Handler() http.Handler {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Printf("dashboard: failed to get static sub-fs: %v", err)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "dashboard assets unavailable", http.StatusInternalServerError)
		})
	}
	return http.FileServer(http.FS(sub))
}
