package music

// Catalog is the server catalog.json contract. Slice order is display order;
// empty arrays represent unavailable service. T2 owns loading and cross-file
// validation, including version equality, ISRC uniqueness, and playable pools.
type Catalog struct {
	Version         string     `json:"version"`
	Decades         []string   `json:"decades"`
	Genres          []Genre    `json:"genres"`
	AllowedRequests []Category `json:"allowedRequests"`
	Pools           []Pool     `json:"pools"`
}

type Genre struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type Pool struct {
	Category Category `json:"category"`
	SongIDs  []string `json:"songIDs"`
}

// CatalogProjection describes the public data format only; no client is built
// here. It must never contain song IDs, titles, answers, or manifest contents.
type CatalogProjection struct {
	Version         string           `json:"version"`
	Decades         []string         `json:"decades"`
	Genres          []Genre          `json:"genres"`
	AllowedRequests []Category       `json:"allowedRequests"`
	Pools           []PoolProjection `json:"pools"`
}

type PoolProjection struct {
	Category  Category `json:"category"`
	SongCount int      `json:"songCount"`
}

type Manifest struct {
	CatalogVersion string         `json:"catalogVersion"`
	Songs          []ManifestSong `json:"songs"`
}

// ReleaseDate is a YYYY-MM-DD string, not the legacy Song time.Time encoding.
// ISRC is a 12-character uppercase standard ISRC; MBID is a canonical UUID.
type ManifestSong struct {
	ISRC        string `json:"isrc"`
	MBID        string `json:"mbid"`
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	ReleaseDate string `json:"releaseDate"`
	Duration    int    `json:"duration"`
	StartTime   int    `json:"startTime"`
}
