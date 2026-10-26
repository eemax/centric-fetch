package pyjson_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"testing"

	"github.com/eemax/centric-fetch/internal/pyjson"
	_ "modernc.org/sqlite"
)

// TestCompatDatabase re-encodes every payload in an existing database and checks the bytes
// and hashes match what's stored:
//
//	CENTRIC_FETCH_COMPAT_DB=/path/to/centric.db go test ./internal/pyjson -run Compat -v
func TestCompatDatabase(t *testing.T) {
	path := os.Getenv("CENTRIC_FETCH_COMPAT_DB")
	if path == "" {
		t.Skip("CENTRIC_FETCH_COMPAT_DB not set")
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT endpoint, id, payload, payload_sha256 FROM bronze_records`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var total, payloadDiff, hashDiff int
	for rows.Next() {
		var endpoint, id, payload, hash string
		if err := rows.Scan(&endpoint, &id, &payload, &hash); err != nil {
			t.Fatal(err)
		}
		total++
		v, err := pyjson.Parse([]byte(payload))
		if err != nil {
			t.Fatalf("%s/%s: %v", endpoint, id, err)
		}
		if got := string(pyjson.Encode(v, false)); got != payload {
			if payloadDiff++; payloadDiff <= 3 {
				t.Errorf("%s/%s payload differs:\n got %.300s\nwant %.300s", endpoint, id, got, payload)
			}
		}
		sum := sha256.Sum256(pyjson.Encode(v, true))
		if hex.EncodeToString(sum[:]) != hash {
			if hashDiff++; hashDiff <= 3 {
				t.Errorf("%s/%s hash differs", endpoint, id)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d records: %d payloads and %d hashes differ", total, payloadDiff, hashDiff)
}
