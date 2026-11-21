package sessioncontract_test

import (
	"testing"

	"temp-ws/internal/sessioncontract"
)

func TestPublicState(t *testing.T) {
	for _, tc := range []struct{ internal, public sessioncontract.RoomState }{
		{sessioncontract.RoomWaiting, sessioncontract.RoomWaiting},
		{sessioncontract.RoomStarting, sessioncontract.RoomPlaying},
		{sessioncontract.RoomPlaying, sessioncontract.RoomPlaying},
		{sessioncontract.RoomResult, sessioncontract.RoomResult},
	} {
		if got := tc.internal.PublicState(); got != tc.public {
			t.Errorf("%s maps to %s, want %s", tc.internal, got, tc.public)
		}
	}
}
