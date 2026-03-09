// crabregistry: a minimal Cargo sparse registry backed by Google Cloud Storage.
//
// Implements the Cargo sparse registry protocol:
//   GET  /index/config.json              → registry config
//   GET  /index/{prefix}/{name}          → newline-delimited index entries
//   PUT  /api/v1/crates/new             → publish a crate (cargo publish)
//   GET  /api/v1/crates/{name}/{ver}/download → download .crate file
//
// GCS layout:
//   {bucket}/index/config.json
//   {bucket}/index/{prefix}/{name}       (index entries, newline-delimited JSON)
//   {bucket}/crates/{name}/{name}-{ver}.crate
//
// Environment variables:
//   GCS_BUCKET  (required) GCS bucket name
//   BASE_URL    (required) Public base URL of this server, e.g. https://crabregistry.internal
//   PORT        (default: 8080)
//   AUTH_TOKEN  (optional) Shared Bearer token required on all requests

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
)

type IndexEntry struct {
	Name     string              `json:"name"`
	Vers     string              `json:"vers"`
	Deps     []IndexDep          `json:"deps"`
	Cksum    string              `json:"cksum"`
	Features map[string][]string `json:"features"`
	Yanked   bool                `json:"yanked"`
	Links    *string             `json:"links"`
}

type IndexDep struct {
	Name            string   `json:"name"`
	Req             string   `json:"req"`
	Features        []string `json:"features"`
	Optional        bool     `json:"optional"`
	DefaultFeatures bool     `json:"default_features"`
	Target          *string  `json:"target"`
	Kind            string   `json:"kind"`
	Registry        *string  `json:"registry,omitempty"`
	Package         *string  `json:"package,omitempty"`
}

// Cargo publish wire format: JSON metadata sent by `cargo publish`
type PublishMeta struct {
	Name        string              `json:"name"`
	Vers        string              `json:"vers"`
	Deps        []PublishDep        `json:"deps"`
	Features    map[string][]string `json:"features"`
	Links       *string             `json:"links"`
	Authors     []string            `json:"authors"`
	Description *string             `json:"description"`
	Homepage    *string             `json:"homepage"`
	License     *string             `json:"license"`
	LicenseFile *string             `json:"license_file"`
	Repository  *string             `json:"repository"`
}

type PublishDep struct {
	Name               string   `json:"name"`
	VersionReq         string   `json:"version_req"`
	Features           []string `json:"features"`
	Optional           bool     `json:"optional"`
	DefaultFeatures    bool     `json:"default_features"`
	Target             *string  `json:"target"`
	Kind               string   `json:"kind"`
	Registry           *string  `json:"registry"`
	ExplicitNameInToml *string  `json:"explicit_name_in_toml"`
}

type Registry struct {
	bucket    *storage.BucketHandle
	baseURL   string
	authToken string // optional shared secret
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// /healthz is unauthenticated — used by k8s liveness/readiness probes
	if req.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.authToken != "" {
		got := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		if got != r.authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	switch {
	case strings.HasPrefix(req.URL.Path, "/index/"):
		r.handleIndex(w, req)
	case req.URL.Path == "/api/v1/crates/new":
		r.handlePublish(w, req)
	case strings.HasPrefix(req.URL.Path, "/api/v1/crates/"):
		r.handleDownload(w, req)
	default:
		http.NotFound(w, req)
	}
}

// GET /index/** — serve index files from GCS
func (r *Registry) handleIndex(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := "index/" + strings.TrimPrefix(req.URL.Path, "/index/")
	obj := r.bucket.Object(key)

	reader, err := obj.NewReader(req.Context())
	if errors.Is(err, storage.ErrObjectNotExist) {
		http.NotFound(w, req)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	if strings.HasSuffix(key, ".json") {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	io.Copy(w, reader)
}

// PUT /api/v1/crates/new — cargo publish wire protocol
func (r *Registry) handlePublish(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := req.Context()

	// Cargo publish body: [u32 json_len][json][u32 crate_len][crate]
	var jsonLen uint32
	if err := binary.Read(req.Body, binary.LittleEndian, &jsonLen); err != nil {
		http.Error(w, "bad request: json length: "+err.Error(), http.StatusBadRequest)
		return
	}
	jsonBytes := make([]byte, jsonLen)
	if _, err := io.ReadFull(req.Body, jsonBytes); err != nil {
		http.Error(w, "bad request: json body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var meta PublishMeta
	if err := json.Unmarshal(jsonBytes, &meta); err != nil {
		http.Error(w, "bad request: json parse: "+err.Error(), http.StatusBadRequest)
		return
	}

	var crateLen uint32
	if err := binary.Read(req.Body, binary.LittleEndian, &crateLen); err != nil {
		http.Error(w, "bad request: crate length: "+err.Error(), http.StatusBadRequest)
		return
	}
	crateBytes := make([]byte, crateLen)
	if _, err := io.ReadFull(req.Body, crateBytes); err != nil {
		http.Error(w, "bad request: crate body: "+err.Error(), http.StatusBadRequest)
		return
	}

	sum := sha256.Sum256(crateBytes)
	cksum := hex.EncodeToString(sum[:])

	// Store .crate in GCS
	cratePath := fmt.Sprintf("crates/%s/%s-%s.crate", meta.Name, meta.Name, meta.Vers)
	if err := r.gcsWrite(ctx, cratePath, "application/octet-stream", crateBytes); err != nil {
		http.Error(w, "store crate: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build and append index entry
	deps := make([]IndexDep, 0, len(meta.Deps))
	for _, d := range meta.Deps {
		deps = append(deps, IndexDep{
			Name:            d.Name,
			Req:             d.VersionReq,
			Features:        d.Features,
			Optional:        d.Optional,
			DefaultFeatures: d.DefaultFeatures,
			Target:          d.Target,
			Kind:            d.Kind,
			Registry:        d.Registry,
			Package:         d.ExplicitNameInToml,
		})
	}
	entry := IndexEntry{
		Name:     meta.Name,
		Vers:     meta.Vers,
		Deps:     deps,
		Cksum:    cksum,
		Features: meta.Features,
		Links:    meta.Links,
	}
	entryJSON, _ := json.Marshal(entry)

	if err := r.appendIndex(ctx, indexPath(meta.Name), entryJSON); err != nil {
		http.Error(w, "update index: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("published %s v%s (cksum=%s...)", meta.Name, meta.Vers, cksum[:12])

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"warnings": map[string]any{
			"invalid_categories": []string{},
			"invalid_badges":     []string{},
			"other":              []string{},
		},
	})
}

// GET /api/v1/crates/{name}/{version}/download
func (r *Registry) handleDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// /api/v1/crates/{name}/{version}/download
	parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/api/v1/crates/"), "/")
	if len(parts) != 3 || parts[2] != "download" {
		http.NotFound(w, req)
		return
	}
	name, version := parts[0], parts[1]
	cratePath := fmt.Sprintf("crates/%s/%s-%s.crate", name, name, version)

	reader, err := r.bucket.Object(cratePath).NewReader(req.Context())
	if errors.Is(err, storage.ErrObjectNotExist) {
		http.NotFound(w, req)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="%s-%s.crate"`, name, version))
	io.Copy(w, reader)
}

// indexPath returns the GCS object key for a crate's index file.
// Follows the Cargo sparse registry naming convention.
func indexPath(name string) string {
	n := strings.ToLower(name)
	switch len(n) {
	case 1:
		return fmt.Sprintf("index/1/%s", n)
	case 2:
		return fmt.Sprintf("index/2/%s", n)
	case 3:
		return fmt.Sprintf("index/3/%s/%s", string(n[0]), n)
	default:
		return fmt.Sprintf("index/%s/%s/%s", n[:2], n[2:4], n)
	}
}

// appendIndex reads the existing index file from GCS, appends the new entry,
// and writes it back. GCS has no native append, so read-modify-write is used.
func (r *Registry) appendIndex(ctx context.Context, path string, entry []byte) error {
	obj := r.bucket.Object(path)

	var existing []byte
	reader, err := obj.NewReader(ctx)
	if err == nil {
		existing, err = io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return fmt.Errorf("read index: %w", err)
		}
	} else if !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("stat index: %w", err)
	}

	if len(existing) > 0 && existing[len(existing)-1] != '\n' {
		existing = append(existing, '\n')
	}
	existing = append(existing, entry...)
	existing = append(existing, '\n')

	return r.gcsWrite(ctx, path, "text/plain; charset=utf-8", existing)
}

func (r *Registry) gcsWrite(ctx context.Context, path, contentType string, data []byte) error {
	w := r.bucket.Object(path).NewWriter(ctx)
	w.ContentType = contentType
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func main() {
	bucket := os.Getenv("GCS_BUCKET")
	if bucket == "" {
		log.Fatal("GCS_BUCKET env var required")
	}
	baseURL := strings.TrimRight(os.Getenv("BASE_URL"), "/")
	if baseURL == "" {
		log.Fatal("BASE_URL env var required (e.g. https://crabregistry.internal:8080)")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}

	reg := &Registry{
		bucket:    client.Bucket(bucket),
		baseURL:   baseURL,
		authToken: os.Getenv("AUTH_TOKEN"),
	}

	// Write config.json to GCS on every startup so it stays in sync with auth settings.
	// "auth-required": true tells Cargo to send credentials on ALL requests (index + api),
	// not just the publish API endpoint. Required when AUTH_TOKEN is set.
	configKey := "index/config.json"
	var config string
	if reg.authToken != "" {
		config = fmt.Sprintf(`{"dl":"%s/api/v1/crates","api":"%s","auth-required":true}`, baseURL, baseURL)
	} else {
		config = fmt.Sprintf(`{"dl":"%s/api/v1/crates","api":"%s"}`, baseURL, baseURL)
	}
	if err := reg.gcsWrite(ctx, configKey, "application/json", []byte(config)); err != nil {
		log.Fatalf("write config.json: %v", err)
	}
	log.Printf("wrote gs://%s/%s (auth-required=%v)", bucket, configKey, reg.authToken != "")

	log.Printf("cargo-registry listening on :%s  bucket=gs://%s  base=%s", port, bucket, baseURL)
	log.Fatal(http.ListenAndServe(":"+port, reg))
}
