package criteria_test

import (
	"testing"

	"temp-ws/internal/matchmaking/criteria"
	music "temp-ws/internal/music/v1"
)

func TestMergeAndJoinEveryAxis(t *testing.T) {
	// Explicit truth tables: rows/columns are any, first, second (plus all
	// for decade). Empty cells mean incompatibility, not an empty category.
	for _, axis := range []struct {
		name   string
		values []string
		merge  [][]string
		set    func(*music.Category, string)
	}{
		{"decade", []string{"any", "2010s", "2020s", "all"}, [][]string{
			{"all", "2010s", "2020s", "all"}, {"2010s", "2010s", "", ""},
			{"2020s", "", "2020s", ""}, {"all", "", "", "all"},
		}, func(c *music.Category, s string) { c.Decade = s }},
		{"artistType", []string{"any", "solo", "group"}, [][]string{
			{"all", "solo", "group"}, {"solo", "solo", ""}, {"group", "", "group"},
		}, func(c *music.Category, s string) { c.ArtistType = s }},
		{"gender", []string{"any", "male", "female"}, [][]string{
			{"all", "male", "female"}, {"male", "male", ""}, {"female", "", "female"},
		}, func(c *music.Category, s string) { c.Gender = s }},
		{"genre", []string{"any", "korean_ballad", "synthetic_pop"}, [][]string{
			{"all", "korean_ballad", "synthetic_pop"}, {"korean_ballad", "korean_ballad", ""}, {"synthetic_pop", "", "synthetic_pop"},
		}, func(c *music.Category, s string) { c.Genre = s }},
	} {
		t.Run(axis.name, func(t *testing.T) {
			anyRequest := music.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "any"}
			allRoom := music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}
			for i, a := range axis.values {
				for j, b := range axis.values {
					t.Run(a+"+"+b, func(t *testing.T) {
						left, right := anyRequest, anyRequest
						axis.set(&left, a)
						axis.set(&right, b)
						wantValue := axis.merge[i][j]
						got, ok := criteria.MergeRequests(left, right)
						if ok != (wantValue != "") {
							t.Fatalf("compatible=%v, want %q", ok, wantValue)
						}
						if !ok {
							if got != (music.Category{}) {
								t.Fatal("partial result on failure")
							}
							return
						}
						want := allRoom
						axis.set(&want, wantValue)
						if got != want || got.ValidateResolved() != nil {
							t.Fatalf("got %+v, want %+v", got, want)
						}
						if !criteria.CanJoin(left, got) || !criteria.CanJoin(right, got) {
							t.Fatal("participants cannot join resolved room")
						}
					})
				}
			}
			// Requests: any, first, second, optionally all. Rooms: all, first, second.
			join := [][]bool{{true, true, true}, {false, true, false}, {false, false, true}, {true, false, false}}
			for i, value := range axis.values {
				for j, roomValue := range []string{"all", axis.values[1], axis.values[2]} {
					request, room := anyRequest, allRoom
					axis.set(&request, value)
					axis.set(&room, roomValue)
					if criteria.CanJoin(request, room) != join[i][j] {
						t.Errorf("join %s -> %s", value, roomValue)
					}
				}
			}
		})
	}
}

func TestInvalidCategoriesFailClosed(t *testing.T) {
	valid := music.Category{Decade: "any", ArtistType: "any", Gender: "any", Genre: "any"}
	room := music.Category{Decade: "all", ArtistType: "all", Gender: "all", Genre: "all"}
	for _, invalid := range []music.Category{
		{}, {Decade: "2010", ArtistType: "any", Gender: "any", Genre: "any"},
		{Decade: "any", ArtistType: "all", Gender: "any", Genre: "any"},
		{Decade: "any", ArtistType: "any", Gender: "all", Genre: "any"},
		{Decade: "any", ArtistType: "any", Gender: "any", Genre: "all"},
	} {
		if _, ok := criteria.MergeRequests(invalid, valid); ok {
			t.Fatal("invalid left accepted")
		}
		if _, ok := criteria.MergeRequests(valid, invalid); ok {
			t.Fatal("invalid right accepted")
		}
		if criteria.CanJoin(invalid, room) {
			t.Fatal("invalid request accepted")
		}
	}
	if criteria.CanJoin(valid, valid) {
		t.Fatal("room contains any")
	}
}
