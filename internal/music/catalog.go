package music

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"regexp"
	"slices"
	"time"

	"github.com/google/uuid"
	"temp-ws/internal/matchmaking/criteria"
	musicv1 "temp-ws/internal/music/v1"
)

var (
	ErrInvalidCatalog  = errors.New("invalid catalog")
	ErrInvalidCriteria = errors.New("invalid criteria")
	catalogISRC        = regexp.MustCompile(`^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$`)
)

// CatalogStore owns a validated immutable snapshot. Methods can be called by
// multiple actors; callers never receive its internal slices or maps. Loading is
// startup work, while request and exact-pool lookups perform no I/O.
// This manifest-backed provider does not fall back to unreviewed database rows.
type CatalogStore struct {
	catalog  musicv1.Catalog
	requests map[musicv1.Category]bool
	pools    map[musicv1.Category][]musicv1.ManifestSong
}

// LoadCatalog loads the server catalog and same-version manifest atomically.
// Invalid data returns no store. Empty catalog arrays are valid but unavailable.
func LoadCatalog(catalogReader, manifestReader io.Reader) (*CatalogStore, error) {
	var catalog musicv1.Catalog
	var manifest musicv1.Manifest
	if err := decodeCatalogJSON(catalogReader, &catalog); err != nil {
		return nil, fmt.Errorf("%w: catalog: %v", ErrInvalidCatalog, err)
	}
	if err := decodeCatalogJSON(manifestReader, &manifest); err != nil {
		return nil, fmt.Errorf("%w: manifest: %v", ErrInvalidCatalog, err)
	}
	store, err := validateCatalog(catalog, manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
	}
	return store, nil
}

func validateCatalog(c musicv1.Catalog, m musicv1.Manifest) (*CatalogStore, error) {
	if !musicv1.ValidCatalogVersion(c.Version) || m.CatalogVersion != c.Version {
		return nil, errors.New("invalid or mismatched version")
	}
	decades, genres := make(map[string]bool), make(map[string]bool)
	for _, d := range c.Decades {
		probe := musicv1.Category{Decade: d, ArtistType: musicv1.Any, Gender: musicv1.Any, Genre: musicv1.Any}
		if d == musicv1.Any || d == musicv1.All || probe.ValidateRequest() != nil || decades[d] {
			return nil, errors.New("invalid or duplicate decade")
		}
		decades[d] = true
	}
	for _, g := range c.Genres {
		probe := musicv1.Category{Decade: musicv1.Any, ArtistType: musicv1.Any, Gender: musicv1.Any, Genre: g.ID}
		if g.ID == musicv1.Any || probe.ValidateRequest() != nil || genres[g.ID] {
			return nil, errors.New("invalid or duplicate genre")
		}
		genres[g.ID] = true
	}
	knownTags := func(category musicv1.Category) bool {
		return (category.Decade == musicv1.Any || category.Decade == musicv1.All || decades[category.Decade]) &&
			(category.Genre == musicv1.Any || category.Genre == musicv1.All || genres[category.Genre])
	}
	songs := make(map[string]musicv1.ManifestSong)
	for i, song := range m.Songs {
		if err := validateManifestSong(song); err != nil {
			return nil, fmt.Errorf("song[%d]: %w", i, err)
		}
		if _, exists := songs[song.ISRC]; exists {
			return nil, errors.New("duplicate manifest ISRC")
		}
		songs[song.ISRC] = song
	}
	s := &CatalogStore{catalog: c, requests: make(map[musicv1.Category]bool), pools: make(map[musicv1.Category][]musicv1.ManifestSong)}
	for _, pool := range c.Pools {
		if pool.Category.ValidateResolved() != nil || !knownTags(pool.Category) {
			return nil, errors.New("invalid or unsupported pool category")
		}
		if _, exists := s.pools[pool.Category]; exists {
			return nil, errors.New("duplicate pool category")
		}
		seen := make(map[string]bool)
		var tracks []musicv1.ManifestSong
		for _, id := range pool.SongIDs {
			song, exists := songs[id]
			if !exists || seen[id] {
				return nil, errors.New("missing or duplicate pool ISRC")
			}
			seen[id] = true
			tracks = append(tracks, song)
		}
		if len(seen) < musicv1.TracksPerGame {
			return nil, errors.New("pool has fewer than 10 distinct ISRCs")
		}
		s.pools[pool.Category] = tracks
	}
	for _, request := range c.AllowedRequests {
		if request.ValidateRequest() != nil || !knownTags(request) {
			return nil, errors.New("invalid or unsupported allowed request")
		}
		found := false
		for resolved := range s.pools {
			if criteria.CanJoin(request, resolved) {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("allowed request has no playable pool")
		}
		s.requests[request] = true
	}
	return s, nil
}

func validateManifestSong(song musicv1.ManifestSong) error {
	if !catalogISRC.MatchString(song.ISRC) {
		return errors.New("invalid ISRC")
	}
	id, err := uuid.Parse(song.MBID)
	if err != nil || id.String() != song.MBID {
		return errors.New("noncanonical MBID")
	}
	date, err := time.Parse(time.DateOnly, song.ReleaseDate)
	if err != nil || date.Format(time.DateOnly) != song.ReleaseDate {
		return errors.New("invalid releaseDate")
	}
	if song.Title == "" || song.Artist == "" || song.Duration < 40 || song.StartTime < 0 || song.StartTime > song.Duration-30 {
		return errors.New("invalid song metadata or playback bounds")
	}
	return nil
}

func (s *CatalogStore) Version() string { return s.catalog.Version }

// Available requires usable selection data, including the default all-any request.
func (s *CatalogStore) Available() bool {
	anyRequest := musicv1.Category{Decade: musicv1.Any, ArtistType: musicv1.Any, Gender: musicv1.Any, Genre: musicv1.Any}
	return len(s.catalog.Decades) > 0 && len(s.catalog.Genres) > 0 && s.requests[anyRequest] && len(s.pools) > 0
}

// ValidateRequest requires exact membership in allowedRequests, not merely
// compatible axes. Callers map ErrInvalidCriteria to MATCH_FAILED/INVALID_CRITERIA.
func (s *CatalogStore) ValidateRequest(version string, request musicv1.Category) error {
	if version != s.Version() || !s.Available() || !s.requests[request] {
		return ErrInvalidCriteria
	}
	return nil
}

// ResolvePair separates individually valid requests from a playable intersection.
// false with nil error means compatible matching is not currently possible; the
// requests can remain waiting. It never substitutes a narrower or broader pool.
func (s *CatalogStore) ResolvePair(version string, a, b musicv1.Category) (musicv1.Category, bool, error) {
	if err := s.ValidateRequest(version, a); err != nil {
		return musicv1.Category{}, false, err
	}
	if err := s.ValidateRequest(version, b); err != nil {
		return musicv1.Category{}, false, err
	}
	resolved, ok := criteria.MergeRequests(a, b)
	if !ok || !s.HasPool(version, resolved) {
		return musicv1.Category{}, false, nil
	}
	return resolved, true, nil
}

func (s *CatalogStore) CanJoin(version string, request, resolved musicv1.Category) bool {
	return s.ValidateRequest(version, request) == nil && s.HasPool(version, resolved) && criteria.CanJoin(request, resolved)
}

func (s *CatalogStore) HasPool(version string, resolved musicv1.Category) bool {
	return version == s.Version() && len(s.pools[resolved]) >= musicv1.TracksPerGame
}

// FetchSongs selects exactly 10 distinct ISRCs from the fixed, versioned pool.
// Returned values retain the manifest startTime; the game may continue using its
// existing start-time generator. Sampling uses the concurrency-safe global RNG.
func (s *CatalogStore) FetchSongs(ctx context.Context, version string, resolved musicv1.Category) ([]musicv1.ManifestSong, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.HasPool(version, resolved) {
		return nil, ErrInvalidCriteria
	}
	tracks := slices.Clone(s.pools[resolved])
	for i := 0; i < musicv1.TracksPerGame; i++ {
		j := i + rand.IntN(len(tracks)-i)
		tracks[i], tracks[j] = tracks[j], tracks[i]
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tracks[:musicv1.TracksPerGame], nil
}

// Projection returns only public metadata and counts, preserving display order.
func (s *CatalogStore) Projection() musicv1.CatalogProjection {
	p := musicv1.CatalogProjection{Version: s.Version(), Decades: slices.Clone(s.catalog.Decades), Genres: slices.Clone(s.catalog.Genres), AllowedRequests: slices.Clone(s.catalog.AllowedRequests), Pools: make([]musicv1.PoolProjection, 0, len(s.catalog.Pools))}
	for _, pool := range s.catalog.Pools {
		p.Pools = append(p.Pools, musicv1.PoolProjection{Category: pool.Category, SongCount: len(s.pools[pool.Category])})
	}
	return p
}

// ValidateProjection checks a distributed projection against this exact snapshot,
// including version, display order, counts and the absence of private song fields.
func (s *CatalogStore) ValidateProjection(r io.Reader) error {
	var p musicv1.CatalogProjection
	if err := decodeCatalogJSON(r, &p); err != nil {
		return fmt.Errorf("%w: projection: %v", ErrInvalidCatalog, err)
	}
	if !reflect.DeepEqual(p, s.Projection()) {
		return fmt.Errorf("%w: projection differs", ErrInvalidCatalog)
	}
	return nil
}
