package message_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	message "temp-ws/internal/message/v1"
	music "temp-ws/internal/music/v1"
)

const fixtureRoot = "../../../docs/specs/random-matchmaking/fixtures"

type validCase struct {
	Name      string          `json:"name"`
	Direction string          `json:"direction"`
	Message   json.RawMessage `json:"message"`
	Canonical json.RawMessage `json:"canonical"`
}

func readFixture(t *testing.T, name string, target any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}

func direction(t *testing.T, name string) message.Direction {
	t.Helper()
	switch name {
	case "client":
		return message.ClientToServer
	case "server":
		return message.ServerToClient
	default:
		t.Fatalf("unknown fixture direction %q", name)
		return 0
	}
}

func jsonValue(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestValidFixturesRoundTrip(t *testing.T) {
	var cases []validCase
	readFixture(t, "messages-valid.json", &cases)
	seen := make(map[string]bool)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir := direction(t, tc.Direction)
			decoded, err := message.Decode(tc.Message, dir)
			if err != nil {
				t.Fatal(err)
			}
			seen[decoded.Type] = true
			encoded, err := message.Encode(decoded, dir)
			if err != nil {
				t.Fatal(err)
			}
			expected := tc.Canonical
			if expected == nil {
				expected = tc.Message
			}
			if !reflect.DeepEqual(jsonValue(t, encoded), jsonValue(t, expected)) {
				t.Fatalf("roundtrip differs:\n got %s\nwant %s", encoded, expected)
			}
			again, err := message.Decode(encoded, dir)
			if err != nil || !reflect.DeepEqual(again, decoded) {
				t.Fatalf("DTO roundtrip: %v", err)
			}
			if _, err := message.Decode(tc.Message, 1-dir); !errors.Is(err, message.ErrInvalidMessage) {
				t.Fatalf("wrong direction accepted: %v", err)
			}
		})
	}
	for _, typ := range []string{
		message.TypeMatchRequest, message.TypeMatchCancel, message.TypeMatchWaiting, message.TypeMatchCancelled,
		message.TypeMatchFailed, message.TypeWelcome, message.TypeLeaveRoom, message.TypeRoomLeft,
		message.TypeStartGame, message.TypeRoomCommandFailed, message.TypeGameAborted, message.TypePlayerJoined,
		message.TypePlayerLeft, message.TypeHostChanged, message.TypePlayerKicked, message.TypeGameStarted,
		message.TypePreloadSong, message.TypeRoundStarted, message.TypeCountDown, message.TypeWrongAnswer,
		message.TypeRoundResult, message.TypeGameOver, message.TypeReadyToPlay, message.TypeSubmitAnswer, message.TypeKickPlayer,
	} {
		if !seen[typ] {
			t.Errorf("missing fixture for %s", typ)
		}
	}
}

type invalidCase struct {
	Name      string          `json:"name"`
	Base      string          `json:"base"`
	Direction string          `json:"direction"`
	Raw       *string         `json:"raw"`
	Path      []string        `json:"path"`
	Op        string          `json:"op"`
	Value     json.RawMessage `json:"value"`
}

// Mutation fixtures use path components, including decimal array indexes. Raw
// JSON is retained so integer limits/fractions are never rounded through float64.
func mutate(t *testing.T, raw json.RawMessage, path []string, op string, value json.RawMessage) json.RawMessage {
	t.Helper()
	if len(path) == 0 {
		t.Fatal("empty mutation path")
	}
	var result any
	if bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) {
		var list []json.RawMessage
		if err := json.Unmarshal(raw, &list); err != nil {
			t.Fatal(err)
		}
		i, err := strconv.Atoi(path[0])
		if err != nil || i < 0 || i >= len(list) {
			t.Fatal("bad fixture array path")
		}
		if len(path) > 1 {
			list[i] = mutate(t, list[i], path[1:], op, value)
		} else if op == "set" {
			list[i] = value
		} else {
			t.Fatal("unsupported array mutation")
		}
		result = list
	} else {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		key := path[0]
		if len(path) > 1 {
			fields[key] = mutate(t, fields[key], path[1:], op, value)
		} else {
			switch op {
			case "remove":
				delete(fields, key)
			case "set":
				fields[key] = value
			default:
				t.Fatal("bad fixture operation")
			}
		}
		result = fields
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestInvalidFixtures(t *testing.T) {
	var valid []validCase
	var invalid []invalidCase
	readFixture(t, "messages-valid.json", &valid)
	readFixture(t, "messages-invalid.json", &invalid)
	bases := make(map[string]validCase)
	for _, tc := range valid {
		if _, ok := bases[tc.Name]; ok {
			t.Fatal("duplicate valid fixture")
		}
		bases[tc.Name] = tc
	}
	names := make(map[string]bool)
	for _, tc := range invalid {
		t.Run(tc.Name, func(t *testing.T) {
			if names[tc.Name] {
				t.Fatal("duplicate invalid fixture")
			}
			names[tc.Name] = true
			base, exists := bases[tc.Base]
			if !exists && tc.Raw == nil {
				t.Fatalf("unknown base %q", tc.Base)
			}
			raw, dir := base.Message, base.Direction
			if tc.Raw != nil {
				raw = []byte(*tc.Raw)
			}
			if tc.Direction != "" {
				dir = tc.Direction
			}
			if len(tc.Path) > 0 {
				raw = mutate(t, raw, tc.Path, tc.Op, tc.Value)
			}
			if _, err := message.Decode(raw, direction(t, dir)); !errors.Is(err, message.ErrInvalidMessage) {
				t.Fatalf("invalid fixture accepted: %s; err=%v", raw, err)
			}
		})
	}
}

func TestRequiredFieldFixtureCoverage(t *testing.T) {
	var valid []validCase
	var invalid []invalidCase
	readFixture(t, "messages-valid.json", &valid)
	readFixture(t, "messages-invalid.json", &invalid)
	coverage := make(map[string]map[string]bool)
	for _, tc := range invalid {
		kind := tc.Op
		if kind == "set" {
			if string(tc.Value) == "null" {
				kind = "null"
			} else {
				kind = "type"
			}
		}
		key := tc.Base + ":" + strings.Join(tc.Path, "/")
		if coverage[key] == nil {
			coverage[key] = make(map[string]bool)
		}
		coverage[key][kind] = true
	}
	seen := make(map[string]bool)
	for _, tc := range valid {
		m, err := message.Decode(tc.Message, direction(t, tc.Direction))
		if err != nil {
			t.Fatal(err)
		}
		if seen[m.Type] {
			continue
		}
		seen[m.Type] = true
		var walk func(map[string]any, []string)
		walk = func(fields map[string]any, path []string) {
			for key, value := range fields {
				current := append(append([]string{}, path...), key)
				for _, kind := range []string{"remove", "null", "type"} {
					if !coverage[tc.Name+":"+strings.Join(current, "/")][kind] {
						t.Errorf("missing %s fixture for %s/%s", kind, tc.Name, strings.Join(current, "/"))
					}
				}
				if nested, ok := value.(map[string]any); ok && key != "scores" {
					walk(nested, current)
				}
			}
		}
		walk(jsonValue(t, tc.Message).(map[string]any), nil)
	}
}

func TestCriteriaErrorsRemainDecodable(t *testing.T) {
	var cases []struct {
		Name    string          `json:"name"`
		Reason  string          `json:"reason"`
		Message json.RawMessage `json:"message"`
	}
	var catalog music.Catalog
	readFixture(t, "criteria-invalid.json", &cases)
	readFixture(t, "catalog.json", &catalog)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			m, err := message.Decode(tc.Message, message.ClientToServer)
			if err != nil {
				t.Fatalf("criteria must reach MATCH_FAILED, not 1008: %v", err)
			}
			p := m.Payload.(*message.MatchRequestPayload)
			validValues := p.Category.ValidateRequest() == nil && music.ValidCatalogVersion(p.CatalogVersion)
			switch tc.Reason {
			case "value":
				if validValues {
					t.Fatal("invalid criteria values accepted")
				}
			case "catalog":
				if !validValues {
					t.Fatal("catalog case must have syntactically valid criteria")
				}
				// Fixture membership only. T2 supplies the production loader/validator.
				if p.CatalogVersion == catalog.Version && slices.Contains(catalog.AllowedRequests, p.Category) {
					t.Fatal("fixture criteria unexpectedly supported")
				}
			default:
				t.Fatal("unknown criteria fixture reason")
			}
		})
	}
}

func TestCodecBoundaries(t *testing.T) {
	var cases []validCase
	readFixture(t, "messages-valid.json", &cases)
	valid := cases[0].Message
	for name, raw := range map[string][]byte{"invalid UTF8": append(append([]byte{}, valid...), 0xff), "fractional timestamp": []byte(`{"type":"MATCH_CANCEL","payload":{"requestID":"11111111-1111-4111-8111-111111111111"},"timestamp":1.0}`)} {
		t.Run(name, func(t *testing.T) {
			if _, err := message.Decode(raw, message.ClientToServer); !errors.Is(err, message.ErrInvalidMessage) {
				t.Fatal(err)
			}
		})
	}
	if _, err := message.Decode(valid, message.Direction(99)); !errors.Is(err, message.ErrInvalidMessage) {
		t.Fatal("invalid direction accepted")
	}
	for i, m := range []message.Message{
		{Type: message.TypeMatchCancel, Payload: nil},
		{Type: message.TypeMatchCancel, Payload: (*message.MatchCancelPayload)(nil)},
		{Type: message.TypeMatchCancel, Payload: message.RoomPayload{RoomID: "22222222-2222-4222-8222-222222222222"}},
		{Type: "UNKNOWN", Payload: message.RequestPayload{}},
		{Type: message.TypeMatchCancel, Payload: message.RequestPayload{RequestID: "bad-id"}},
	} {
		t.Run(fmt.Sprintf("encode-%d", i), func(t *testing.T) {
			if _, err := message.Encode(m, message.ClientToServer); !errors.Is(err, message.ErrInvalidMessage) {
				t.Fatalf("bad DTO accepted: %v", err)
			}
		})
	}
	// Both values and pointers are supported for callers constructing outbound DTOs.
	m := message.Message{Type: message.TypeMatchCancel, Payload: message.MatchCancelPayload{RequestID: "11111111-1111-4111-8111-111111111111"}}
	if _, err := message.Encode(m, message.ClientToServer); err != nil {
		t.Fatal(err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{"type":"MATCH_CANCEL","payload":{"requestID":"11111111-1111-4111-8111-111111111111"}}`), false)
	f.Add([]byte(`{"type":"GAME_STARTED","payload":{"roomID":"22222222-2222-4222-8222-222222222222"},"timestamp":0}`), true)
	f.Fuzz(func(t *testing.T, raw []byte, server bool) {
		dir := message.ClientToServer
		if server {
			dir = message.ServerToClient
		}
		m, err := message.Decode(raw, dir)
		if err != nil {
			return
		}
		encoded, err := message.Encode(m, dir)
		if err != nil {
			t.Fatal(err)
		}
		again, err := message.Decode(encoded, dir)
		if err != nil || !reflect.DeepEqual(m, again) {
			t.Fatalf("roundtrip: %v", err)
		}
	})
}
