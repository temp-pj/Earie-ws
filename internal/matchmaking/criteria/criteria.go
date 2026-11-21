// Package criteria implements pure v1 request compatibility. Catalog membership
// and availability are checked separately by the music catalog store.
package criteria

import music "temp-ws/internal/music/v1"

// MergeRequests resolves compatible requests to a fixed room category. The bool
// reports syntactic compatibility only; the exact resolved pool must also exist.
func MergeRequests(a, b music.Category) (music.Category, bool) {
	if a.ValidateRequest() != nil || b.ValidateRequest() != nil {
		return music.Category{}, false
	}
	var result music.Category
	for _, axis := range []struct {
		a, b string
		out  *string
	}{
		{a.Decade, b.Decade, &result.Decade},
		{a.ArtistType, b.ArtistType, &result.ArtistType},
		{a.Gender, b.Gender, &result.Gender},
		{a.Genre, b.Genre, &result.Genre},
	} {
		switch {
		case axis.a == music.Any && axis.b == music.Any:
			*axis.out = music.All
		case axis.a == music.Any:
			*axis.out = axis.b
		case axis.b == music.Any || axis.a == axis.b:
			*axis.out = axis.a
		default:
			return music.Category{}, false
		}
	}
	return result, true
}

// CanJoin never narrows a room: only any or an identical value accepts an axis.
// In particular, a specific decade cannot join an all-decades room.
func CanJoin(request, resolved music.Category) bool {
	if request.ValidateRequest() != nil || resolved.ValidateResolved() != nil {
		return false
	}
	for _, axis := range [][2]string{
		{request.Decade, resolved.Decade}, {request.ArtistType, resolved.ArtistType},
		{request.Gender, resolved.Gender}, {request.Genre, resolved.Genre},
	} {
		if axis[0] != music.Any && axis[0] != axis[1] {
			return false
		}
	}
	return true
}
