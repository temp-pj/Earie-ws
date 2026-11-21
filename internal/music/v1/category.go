// Package music defines the v1 catalog value contract. It intentionally does not
// load catalogs, query songs, or decide whether requests have a playable pool (T2).
// The legacy internal/music.Category remains in use until the runtime migration.
package music

import (
	"fmt"
	"regexp"
)

const (
	Any           = "any"
	All           = "all"
	ArtistSolo    = "solo"
	ArtistGroup   = "group"
	GenderMale    = "male"
	GenderFemale  = "female"
	TracksPerGame = 10
)

// Category is shared by requests, resolved rooms, and catalog pools.
type Category struct {
	Decade     string `json:"decade"`
	ArtistType string `json:"artistType"`
	Gender     string `json:"gender"`
	Genre      string `json:"genre"`
}

var (
	decadePattern  = regexp.MustCompile(`^[0-9]{4}s$`)
	genrePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	versionPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

func ValidCatalogVersion(version string) bool { return versionPattern.MatchString(version) }

// ValidateRequest checks values only, not catalog membership or pool eligibility.
// Invalid values are INVALID_CRITERIA, not malformed wire messages.
func (c Category) ValidateRequest() error { return c.validate(false) }

// ValidateResolved checks the shape of a fixed room/pool category (no any).
func (c Category) ValidateResolved() error { return c.validate(true) }

func (c Category) validate(resolved bool) error {
	validDecade := c.Decade == All || (c.Decade == Any && !resolved) || decadePattern.MatchString(c.Decade)
	if !validDecade {
		return fmt.Errorf("invalid decade %q", c.Decade)
	}
	allowedWildcard := Any
	if resolved {
		allowedWildcard = All
	}
	if c.ArtistType != allowedWildcard && c.ArtistType != ArtistSolo && c.ArtistType != ArtistGroup {
		return fmt.Errorf("invalid artistType %q", c.ArtistType)
	}
	if c.Gender != allowedWildcard && c.Gender != GenderMale && c.Gender != GenderFemale {
		return fmt.Errorf("invalid gender %q", c.Gender)
	}
	if c.Genre != allowedWildcard && (c.Genre == Any || c.Genre == All || !genrePattern.MatchString(c.Genre)) {
		return fmt.Errorf("invalid genre %q", c.Genre)
	}
	return nil
}
