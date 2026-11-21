package music_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"temp-ws/internal/matchmaking/criteria"
	message "temp-ws/internal/message/v1"
	"temp-ws/internal/music"
	musicv1 "temp-ws/internal/music/v1"
)

const catalogFixtures = "../../docs/specs/random-matchmaking/fixtures"

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(catalogFixtures, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func encode(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func syntheticStore(t *testing.T) *music.CatalogStore {
	t.Helper()
	s, err := music.LoadCatalog(bytes.NewReader(fixture(t, "catalog.json")), bytes.NewReader(fixture(t, "manifest.json")))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func catalogValue(t *testing.T) musicv1.Catalog {
	t.Helper()
	var c musicv1.Catalog
	if err := json.Unmarshal(fixture(t, "catalog.json"), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSyntheticCatalogThroughProductionLoader(t *testing.T) {
	s := syntheticStore(t)
	if s.Version() != "test-v1" || !s.Available() {
		t.Fatal("synthetic catalog unavailable")
	}
	if err := s.ValidateProjection(bytes.NewReader(fixture(t, "catalog-projection.json"))); err != nil {
		t.Fatal(err)
	}
	c := catalogValue(t)
	for _, request := range c.AllowedRequests {
		if err := s.ValidateRequest(c.Version, request); err != nil {
			t.Fatal(err)
		}
	}
	for _, pool := range c.Pools {
		tracks, err := s.FetchSongs(context.Background(), c.Version, pool.Category)
		if err != nil {
			t.Fatal(err)
		}
		if len(tracks) != musicv1.TracksPerGame {
			t.Fatalf("got %d tracks", len(tracks))
		}
		seen := make(map[string]bool)
		for _, song := range tracks {
			if seen[song.ISRC] {
				t.Fatal("repeated ISRC")
			}
			seen[song.ISRC] = true
			found := false
			for _, id := range pool.SongIDs {
				found = found || id == song.ISRC
			}
			if !found || song.Title == "" || song.MBID == "" {
				t.Fatal("wrong pool or incomplete manifest song")
			}
		}
	}
	resolved, ok, err := s.ResolvePair(c.Version, c.AllowedRequests[0], c.AllowedRequests[3])
	if err != nil || !ok || resolved != c.Pools[2].Category {
		t.Fatalf("pair: %+v %v %v", resolved, ok, err)
	}
	if !s.CanJoin(c.Version, c.AllowedRequests[0], resolved) || s.CanJoin(c.Version, c.AllowedRequests[3], c.Pools[0].Category) {
		t.Fatal("join changed room criteria")
	}
	if _, ok, err := s.ResolvePair(c.Version, c.AllowedRequests[3], c.AllowedRequests[4]); ok || err != nil {
		t.Fatal("conflicting requests paired")
	}
	if _, ok, err := s.ResolvePair("test-v2", c.AllowedRequests[0], c.AllowedRequests[0]); ok || !errors.Is(err, music.ErrInvalidCriteria) {
		t.Fatal("wrong version paired")
	}
}

func TestIndividuallyValidRequestsNeedExactResolvedPool(t *testing.T) {
	c := catalogValue(t)
	// Each request has 10 songs, but the supporting pools share only nine
	// ISRCs. No playable resolved pool exists; never substitute their union.
	a := musicv1.Category{Decade: "any", ArtistType: "any", Gender: "female", Genre: "any"}
	b := musicv1.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "korean_ballad"}
	c.AllowedRequests = append(c.AllowedRequests, a, b)
	c.Pools = append(c.Pools,
		musicv1.Pool{Category: musicv1.Category{Decade: "all", ArtistType: "all", Gender: "female", Genre: "all"}, SongIDs: c.Pools[0].SongIDs[:10]},
		musicv1.Pool{Category: musicv1.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "korean_ballad"}, SongIDs: c.Pools[0].SongIDs[1:11]},
	)
	load := func() (*music.CatalogStore, error) {
		return music.LoadCatalog(bytes.NewReader(encode(t, c)), bytes.NewReader(fixture(t, "manifest.json")))
	}
	s, err := load()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []musicv1.Category{a, b} {
		if err := s.ValidateRequest(c.Version, r); err != nil {
			t.Fatal(err)
		}
	}
	resolved, compatible := criteria.MergeRequests(a, b)
	if !compatible {
		t.Fatal("requests should be syntactically compatible")
	}
	if _, ok, err := s.ResolvePair(c.Version, a, b); err != nil || ok {
		t.Fatal("missing intersection was matched")
	}
	c.Pools = append(c.Pools, musicv1.Pool{Category: resolved, SongIDs: c.Pools[0].SongIDs[1:10]})
	if s, err := load(); s != nil || !errors.Is(err, music.ErrInvalidCatalog) {
		t.Fatal("nine-song intersection loaded")
	}
	// Adding a duplicate row cannot turn nine unique songs into a valid pool.
	ids := append([]string{}, c.Pools[len(c.Pools)-1].SongIDs...)
	c.Pools[len(c.Pools)-1].SongIDs = append(ids, ids[0])
	if s, err := load(); s != nil || !errors.Is(err, music.ErrInvalidCatalog) {
		t.Fatal("duplicate ISRC inflated intersection count")
	}
	// A later reviewed dataset supplies ten common, distinct songs.
	c.Pools[len(c.Pools)-2].SongIDs = c.Pools[0].SongIDs[:10]
	c.Pools[len(c.Pools)-1].SongIDs = c.Pools[0].SongIDs[:10]
	s, err = load()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.ResolvePair(c.Version, a, b); err != nil || !ok || got != resolved {
		t.Fatal("playable intersection not matched")
	}
}

func TestCriteriaFixturesThroughWireAndCatalog(t *testing.T) {
	s := syntheticStore(t)
	var cases []struct {
		Name    string
		Message json.RawMessage
	}
	if err := json.Unmarshal(fixture(t, "criteria-invalid.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			msg, err := message.Decode(tc.Message, message.ClientToServer)
			if err != nil {
				t.Fatalf("criteria error must remain a valid message: %v", err)
			}
			request := msg.Payload.(*message.MatchRequestPayload)
			if err := s.ValidateRequest(request.CatalogVersion, request.Category); !errors.Is(err, music.ErrInvalidCriteria) {
				t.Fatal("invalid criteria accepted")
			}
		})
	}
	// Supported individual tags are insufficient: the full tuple must be listed.
	unlisted := musicv1.Category{Decade: "2020s", ArtistType: "solo", Gender: "female", Genre: "korean_ballad"}
	if err := s.ValidateRequest("test-v1", unlisted); !errors.Is(err, music.ErrInvalidCriteria) {
		t.Fatal("unlisted tuple accepted")
	}
}

func mutateCatalog(t *testing.T, object any, path []string, op string, value any) {
	t.Helper()
	switch current := object.(type) {
	case map[string]any:
		if len(path) > 1 {
			mutateCatalog(t, current[path[0]], path[1:], op, value)
			return
		}
		if op == "remove" {
			delete(current, path[0])
		} else {
			current[path[0]] = value
		}
	case []any:
		i, err := strconv.Atoi(path[0])
		if err != nil {
			t.Fatal(err)
		}
		if len(path) > 1 {
			mutateCatalog(t, current[i], path[1:], op, value)
		} else {
			current[i] = value
		}
	default:
		t.Fatalf("invalid fixture traversal %T", object)
	}
}

func TestT0InvalidCatalogFixturesThroughProductionLoader(t *testing.T) {
	var cases []struct {
		Name, File, Op string
		Path           []string
		Value          json.RawMessage
	}
	if err := json.Unmarshal(fixture(t, "catalog-invalid.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			files := map[string][]byte{"catalog.json": fixture(t, "catalog.json"), "manifest.json": fixture(t, "manifest.json"), "catalog-projection.json": fixture(t, "catalog-projection.json")}
			var object, value any
			if err := json.Unmarshal(files[tc.File], &object); err != nil {
				t.Fatal(err)
			}
			if tc.Op == "set" {
				if err := json.Unmarshal(tc.Value, &value); err != nil {
					t.Fatal(err)
				}
			}
			mutateCatalog(t, object, tc.Path, tc.Op, value)
			files[tc.File] = encode(t, object)
			s, err := music.LoadCatalog(bytes.NewReader(files["catalog.json"]), bytes.NewReader(files["manifest.json"]))
			if tc.File == "catalog-projection.json" {
				if err != nil {
					t.Fatal(err)
				}
				err = s.ValidateProjection(bytes.NewReader(files[tc.File]))
			} else if s != nil {
				t.Fatal("invalid catalog returned a store")
			}
			if !errors.Is(err, music.ErrInvalidCatalog) {
				t.Fatalf("expected invalid catalog: %v", err)
			}
		})
	}
}

func TestUnavailableAndEmptyPools(t *testing.T) {
	raw := fixture(t, "catalog-unavailable.json")
	var c musicv1.Catalog
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	m := musicv1.Manifest{CatalogVersion: c.Version, Songs: []musicv1.ManifestSong{}}
	s, err := music.LoadCatalog(bytes.NewReader(raw), bytes.NewReader(encode(t, m)))
	if err != nil || s.Available() {
		t.Fatalf("unavailable catalog: %v", err)
	}
	anyRequest := musicv1.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "any"}
	if !errors.Is(s.ValidateRequest(c.Version, anyRequest), music.ErrInvalidCriteria) {
		t.Fatal("unavailable accepted request")
	}
	if _, err := s.FetchSongs(context.Background(), c.Version, musicv1.Category{}); !errors.Is(err, music.ErrInvalidCriteria) {
		t.Fatal("unavailable returned songs")
	}
	for _, field := range []string{"decades", "genres", "allowedRequests", "pools"} {
		t.Run("null-"+field, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			fields[field] = json.RawMessage("null")
			if s, err := music.LoadCatalog(bytes.NewReader(encode(t, fields)), bytes.NewReader(encode(t, m))); err == nil || s != nil {
				t.Fatal("null unavailable array accepted")
			}
		})
	}
	c = catalogValue(t)
	c.Pools[0].SongIDs = []string{}
	if s, err := music.LoadCatalog(bytes.NewReader(encode(t, c)), bytes.NewReader(fixture(t, "manifest.json"))); err == nil || s != nil {
		t.Fatal("empty pool accepted")
	}
	// A missing default selection disables matching even with other valid choices.
	c = catalogValue(t)
	c.AllowedRequests = c.AllowedRequests[1:]
	s, err = music.LoadCatalog(bytes.NewReader(encode(t, c)), bytes.NewReader(fixture(t, "manifest.json")))
	if err != nil || s.Available() {
		t.Fatalf("missing default: %v", err)
	}
}

func TestSnapshotIsolationAndConcurrentQueries(t *testing.T) {
	s := syntheticStore(t)
	want := s.Projection()
	p := s.Projection()
	p.Decades[0] = "1900s"
	p.Genres[0].ID = "changed"
	p.AllowedRequests[0].Genre = "changed"
	p.Pools[0].SongCount = 0
	if !reflect.DeepEqual(want, s.Projection()) {
		t.Fatal("projection mutated store")
	}
	// This pool has exactly ten songs, so every call returns every member and
	// isolation assertions do not depend on which random sample was selected.
	room := want.Pools[2].Category
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracks, err := s.FetchSongs(context.Background(), s.Version(), room)
			if err != nil {
				t.Error(err)
				return
			}
			tracks[0].Title = "caller mutation"
			if !s.CanJoin(s.Version(), want.AllowedRequests[0], room) {
				t.Error("join failed")
			}
			if !reflect.DeepEqual(want, s.Projection()) {
				t.Error("snapshot changed")
			}
		}()
	}
	wg.Wait()
	tracks, err := s.FetchSongs(context.Background(), s.Version(), room)
	if err != nil {
		t.Fatal(err)
	}
	for _, track := range tracks {
		if track.Title == "caller mutation" {
			t.Fatal("song storage escaped")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.FetchSongs(ctx, s.Version(), room); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored")
	}
	if _, err := s.FetchSongs(context.Background(), "test-v2", room); !errors.Is(err, music.ErrInvalidCriteria) {
		t.Fatal("wrong version fetched")
	}
}

type brokenCatalogReader struct{}

func (brokenCatalogReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestCatalogReadAndJSONErrors(t *testing.T) {
	for _, r := range []io.Reader{brokenCatalogReader{}, strings.NewReader("{"), strings.NewReader("null"), strings.NewReader(string(fixture(t, "catalog.json")) + " {}"), strings.NewReader(`{"version":"test-v1"}`)} {
		if s, err := music.LoadCatalog(r, bytes.NewReader(fixture(t, "manifest.json"))); !errors.Is(err, music.ErrInvalidCatalog) || s != nil {
			t.Fatal("bad catalog accepted")
		}
	}
	if s, err := music.LoadCatalog(bytes.NewReader(fixture(t, "catalog.json")), brokenCatalogReader{}); !errors.Is(err, music.ErrInvalidCatalog) || s != nil {
		t.Fatal("manifest read failure accepted")
	}
}
