// Package web renders truss's embedded status dashboard.
package web

import (
	_ "embed"
	"fmt"
	"html/template"
	"io"
	"sort"
	"time"

	"github.com/edxeme/trusskv/internal/node"
	"github.com/edxeme/trusskv/internal/store"
)

//go:embed dashboard.html
var dashboardHTML string

var tmpl = template.Must(template.New("dashboard").Parse(dashboardHTML))

// maxPreview bounds the value preview shown per key.
const maxPreview = 64

type recordView struct {
	Key     string
	Value   string
	Version string
}

type view struct {
	Status      node.Status
	Records     []recordView
	GeneratedAt string
}

// Render writes the dashboard for the given status and snapshot.
func Render(w io.Writer, st node.Status, records map[string]store.Record, now time.Time) error {
	v := view{Status: st, GeneratedAt: now.UTC().Format(time.RFC1123)}
	keys := make([]string, 0, len(records))
	for k, rec := range records {
		if !rec.Tombstone {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		rec := records[k]
		value := string(rec.Value)
		if len(value) > maxPreview {
			value = value[:maxPreview] + "…"
		}
		v.Records = append(v.Records, recordView{
			Key:     k,
			Value:   value,
			Version: fmt.Sprintf("term %d · index %d · ts %d", rec.Term, rec.Index, rec.Timestamp),
		})
	}
	return tmpl.Execute(w, v)
}
