package music_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	music "temp-ws/internal/music/v1"
)

const fixtureRoot = "../../../docs/specs/random-matchmaking/fixtures"

func read(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCategoryFixtures(t *testing.T) {
	var cases []struct {
		Name          string         `json:"name"`
		Category      music.Category `json:"category"`
		RequestValid  bool           `json:"requestValid"`
		ResolvedValid bool           `json:"resolvedValid"`
	}
	if err := json.Unmarshal(read(t, "categories.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if got := tc.Category.ValidateRequest() == nil; got != tc.RequestValid {
				t.Errorf("request valid=%v, want %v", got, tc.RequestValid)
			}
			if got := tc.Category.ValidateResolved() == nil; got != tc.ResolvedValid {
				t.Errorf("resolved valid=%v, want %v", got, tc.ResolvedValid)
			}
		})
	}
}

func TestCatalogVersionFixtures(t *testing.T) {
	var cases []struct {
		Value string `json:"value"`
		Valid bool   `json:"valid"`
	}
	if err := json.Unmarshal(read(t, "versions.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		if music.ValidCatalogVersion(tc.Value) != tc.Valid {
			t.Errorf("wrong validity for %q", tc.Value)
		}
	}
}

func jsonValue(raw []byte) (any, error) {
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	err := d.Decode(&value)
	return value, err
}

// exactDTO is a fixture audit, NOT the future production catalog decoder. Public
// fixtures intentionally contain exactly the documented keys. Roundtrip equality
// catches missing keys, wrong types and accidental projection answer leaks.
// Nil slices preserve JSON null, so array contracts need explicit value checks.
func exactDTO(raw []byte, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		return err
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return err
	}
	a, err := jsonValue(raw)
	if err != nil {
		return err
	}
	b, err := jsonValue(encoded)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("fixture does not roundtrip through %T", target)
	}
	return nil
}

var (
	decadePattern = regexp.MustCompile(`^[0-9]{4}s$`)
	genrePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	isrcPattern   = regexp.MustCompile(`^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$`)
)

// auditBundle checks the synthetic contract examples against protocol.md. It is
// deliberately test-only: T2 still owns the real loader, matching/pool validator
// and its rejection paths. Passing this audit does not verify production data.
func auditBundle(files map[string][]byte) error {
	var catalog music.Catalog
	var manifest music.Manifest
	var projection music.CatalogProjection
	for name, target := range map[string]any{"catalog.json": &catalog, "manifest.json": &manifest, "catalog-projection.json": &projection} {
		if err := exactDTO(files[name], target); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if !music.ValidCatalogVersion(catalog.Version) || manifest.CatalogVersion != catalog.Version || projection.Version != catalog.Version {
		return fmt.Errorf("catalog versions differ or are invalid")
	}
	decades := make(map[string]bool)
	for _, d := range catalog.Decades {
		if !decadePattern.MatchString(d) || decades[d] {
			return fmt.Errorf("invalid/duplicate decade")
		}
		decades[d] = true
	}
	genres := make(map[string]bool)
	for _, g := range catalog.Genres {
		if !genrePattern.MatchString(g.ID) || g.ID == music.Any || g.ID == music.All || genres[g.ID] {
			return fmt.Errorf("invalid/duplicate genre")
		}
		genres[g.ID] = true
	}
	knownTags := func(c music.Category) bool {
		return (c.Decade == music.Any || c.Decade == music.All || decades[c.Decade]) && (c.Genre == music.Any || c.Genre == music.All || genres[c.Genre])
	}
	songs := make(map[string]bool)
	for _, song := range manifest.Songs {
		if !isrcPattern.MatchString(song.ISRC) || songs[song.ISRC] {
			return fmt.Errorf("invalid/duplicate ISRC")
		}
		songs[song.ISRC] = true
		id, err := uuid.Parse(song.MBID)
		if err != nil || id.String() != song.MBID {
			return fmt.Errorf("noncanonical MBID")
		}
		date, err := time.Parse("2006-01-02", song.ReleaseDate)
		if err != nil || date.Format("2006-01-02") != song.ReleaseDate {
			return fmt.Errorf("invalid releaseDate")
		}
		if song.Title == "" || song.Artist == "" || song.Duration < 40 || song.StartTime < 0 || song.StartTime > song.Duration-30 {
			return fmt.Errorf("invalid manifest song values")
		}
	}
	poolCategories := make(map[music.Category]bool)
	for _, pool := range catalog.Pools {
		if pool.Category.ValidateResolved() != nil || !knownTags(pool.Category) || poolCategories[pool.Category] {
			return fmt.Errorf("invalid/duplicate pool category")
		}
		poolCategories[pool.Category] = true
		if len(pool.SongIDs) < music.TracksPerGame {
			return fmt.Errorf("short pool")
		}
		seen := make(map[string]bool)
		for _, id := range pool.SongIDs {
			if !songs[id] || seen[id] {
				return fmt.Errorf("missing/duplicate pool song")
			}
			seen[id] = true
		}
	}
	for _, request := range catalog.AllowedRequests {
		if request.ValidateRequest() != nil || !knownTags(request) {
			return fmt.Errorf("invalid request category")
		}
		found := false
		for _, pool := range catalog.Pools {
			pairs := [][2]string{{request.Decade, pool.Category.Decade}, {request.ArtistType, pool.Category.ArtistType}, {request.Gender, pool.Category.Gender}, {request.Genre, pool.Category.Genre}}
			matches := true
			for _, pair := range pairs {
				if pair[0] != music.Any && pair[0] != pair[1] {
					matches = false
				}
			}
			found = found || matches
		}
		if !found {
			return fmt.Errorf("allowed request has no supporting pool")
		}
	}
	if !reflect.DeepEqual(projection.Decades, catalog.Decades) || !reflect.DeepEqual(projection.Genres, catalog.Genres) || !reflect.DeepEqual(projection.AllowedRequests, catalog.AllowedRequests) || len(projection.Pools) != len(catalog.Pools) {
		return fmt.Errorf("projection header/pools differ")
	}
	for i, pool := range projection.Pools {
		if pool.Category != catalog.Pools[i].Category || pool.SongCount != len(catalog.Pools[i].SongIDs) || pool.SongCount < music.TracksPerGame {
			return fmt.Errorf("projection category/count differs")
		}
	}
	return nil
}

func bundle(t *testing.T) map[string][]byte {
	t.Helper()
	return map[string][]byte{"catalog.json": read(t, "catalog.json"), "manifest.json": read(t, "manifest.json"), "catalog-projection.json": read(t, "catalog-projection.json")}
}

func TestSyntheticCatalogContract(t *testing.T) {
	files := bundle(t)
	if err := auditBundle(files); err != nil {
		t.Fatal(err)
	}
	var catalog music.Catalog
	if err := json.Unmarshal(files["catalog.json"], &catalog); err != nil {
		t.Fatal(err)
	}
	if catalog.Version != "test-v1" {
		t.Fatal("fixtures must remain explicitly synthetic")
	}
	if !slices.Equal(catalog.Decades, []string{"2010s", "2020s"}) {
		t.Fatal("display order changed")
	}
	if err := auditUnavailable(read(t, "catalog-unavailable.json")); err != nil {
		t.Fatal(err)
	}
}

func auditUnavailable(raw []byte) error {
	var unavailable music.Catalog
	if err := exactDTO(raw, &unavailable); err != nil {
		return err
	}
	if unavailable.Decades == nil || unavailable.Genres == nil || unavailable.AllowedRequests == nil || unavailable.Pools == nil {
		return fmt.Errorf("unavailable catalog arrays must not be null")
	}
	if len(unavailable.Decades)+len(unavailable.Genres)+len(unavailable.AllowedRequests)+len(unavailable.Pools) != 0 {
		return fmt.Errorf("unavailable catalog must have empty arrays")
	}
	return nil
}

func TestUnavailableCatalogRejectsNullArrays(t *testing.T) {
	raw := read(t, "catalog-unavailable.json")
	t.Run("empty arrays remain valid", func(t *testing.T) {
		if err := auditUnavailable(raw); err != nil {
			t.Fatal(err)
		}
	})
	for _, field := range []string{"decades", "genres", "allowedRequests", "pools"} {
		t.Run(field, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			fields[field] = json.RawMessage("null")
			mutated, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := auditUnavailable(mutated); err == nil {
				t.Fatalf("null %s array accepted", field)
			}
		})
	}
}

func mutate(t *testing.T, object any, path []string, op string, value any) {
	t.Helper()
	if len(path) == 0 {
		t.Fatal("empty mutation path")
	}
	switch current := object.(type) {
	case map[string]any:
		if len(path) > 1 {
			mutate(t, current[path[0]], path[1:], op, value)
			return
		}
		switch op {
		case "remove":
			delete(current, path[0])
		case "set":
			current[path[0]] = value
		default:
			t.Fatal("bad mutation")
		}
	case []any:
		i, err := strconv.Atoi(path[0])
		if err != nil || i < 0 || i >= len(current) {
			t.Fatal("bad index")
		}
		if len(path) > 1 {
			mutate(t, current[i], path[1:], op, value)
		} else if op == "set" {
			current[i] = value
		} else {
			t.Fatal("bad array mutation")
		}
	default:
		t.Fatalf("invalid mutation traversal %T", object)
	}
}

func TestInvalidCatalogFixtureAudit(t *testing.T) {
	var cases []struct {
		Name  string          `json:"name"`
		File  string          `json:"file"`
		Path  []string        `json:"path"`
		Op    string          `json:"op"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(read(t, "catalog-invalid.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			files := bundle(t)
			object, err := jsonValue(files[tc.File])
			if err != nil {
				t.Fatal(err)
			}
			var value any
			if tc.Op == "set" {
				value, err = jsonValue(tc.Value)
				if err != nil {
					t.Fatal(err)
				}
			}
			mutate(t, object, tc.Path, tc.Op, value)
			files[tc.File], err = json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			if err := auditBundle(files); err == nil {
				t.Fatal("invalid catalog fixture accepted by contract audit")
			}
		})
	}
}
