// Package dashboard provides the Web Dashboard for hermes-agent-cluster.
// It embeds the static HTML/CSS/JS files and serves them at /dashboard/.
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFiles embed.FS

// Handler returns an http.Handler that serves the dashboard static files.
// The returned handler strips the "static" prefix so files are served from root.
func Handler() http.Handler {
	// Get the sub-filesystem rooted at "static"
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic("dashboard: failed to get static sub-fs: " + err.Error())
	}
	return http.FileServer(http.FS(sub))
}
