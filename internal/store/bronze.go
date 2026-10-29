package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/eemax/centric-fetch/internal/pyjson"
)

// PageCounts is what storing a page did.
type PageCounts struct {
	Inserted, Updated, Unchanged, Restored int
	// Records not yet seen in this window that were last modified before it started: records
	// an earlier pass over the same window should have returned.
	LateFinds int
	// Records modified while the window was being read (after its start, less the overlap).
	Concurrent int
	// Updated records whose _modified_at didn't move (or that have none): Centric changed them
	// silently, which an incremental fetch can't see. SilentFields counts the top-level fields
	// that differed, over the SilentSampled records that were compared field by field.
	Silent, SilentSampled int
	SilentFields          map[string]int
}

// Add sums o into c.
func (c *PageCounts) Add(o PageCounts) {
	c.Inserted += o.Inserted
	c.Updated += o.Updated
	c.Unchanged += o.Unchanged
	c.Restored += o.Restored
	c.LateFinds += o.LateFinds
	c.Concurrent += o.Concurrent
	c.Silent += o.Silent
	c.SilentSampled += o.SilentSampled
	for k, n := range o.SilentFields {
		if c.SilentFields == nil {
			c.SilentFields = map[string]int{}
		}
		c.SilentFields[k] += n
	}
}

// BadRecord is a record Centric sent that can't be stored (not an object, or no usable id).
type BadRecord struct{ Msg string }

func (e *BadRecord) Error() string { return e.Msg }

// Page is one page of records to store.
type Page struct {
	Endpoint  string
	Items     []pyjson.Value
	RunID     string
	Now       string
	ChangeSeq int64
	// Set on verification passes: records modified before this that the window hadn't seen
	// yet count as late finds.
	WindowStartedAt string
	// Records modified at or after this count as concurrent changes ("" to skip).
	ChangedSince string
	// At most this many silent changes on the page are compared field by field (each costs a
	// parse of the stored payload); the rest are only counted.
	SilentSample int
}

// ApplyPage upserts a page. A stored payload is only replaced by one with the same or a
// newer _modified_at, so overlapping or out-of-order pages never roll a record back. Every
// record seen is stamped with the run id, which full fetches use to find deletions.
//
// Most records a full fetch reads haven't changed, so the hash comes first and the stored
// form is only built for a record that's written.
func ApplyPage(tx *sql.Tx, p Page) (PageCounts, error) {
	var counts PageCounts
	stmts, err := prepare(tx, map[string]string{
		"select": `SELECT modified_at, payload_sha256, deleted, last_seen_run
			FROM bronze_records WHERE endpoint = ? AND id = ?`,
		"insert": `INSERT INTO bronze_records (endpoint, id, modified_at, payload,
			payload_sha256, deleted, deleted_at, first_seen_at, updated_at, last_seen_at,
			last_seen_run, change_seq) VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?, ?)`,
		"update": `UPDATE bronze_records SET modified_at = ?, payload = ?,
			payload_sha256 = ?, deleted = 0, deleted_at = NULL, updated_at = ?,
			last_seen_at = ?, last_seen_run = ?, change_seq = ? WHERE endpoint = ? AND id = ?`,
		"restore": `UPDATE bronze_records SET deleted = 0, deleted_at = NULL,
			updated_at = ?, last_seen_at = ?, last_seen_run = ?, change_seq = ?
			WHERE endpoint = ? AND id = ?`,
		"seen": `UPDATE bronze_records SET last_seen_at = ?, last_seen_run = ?
			WHERE endpoint = ? AND id = ?`,
		"payload": `SELECT payload FROM bronze_records WHERE endpoint = ? AND id = ?`,
	})
	if err != nil {
		return counts, err
	}
	defer func() {
		for _, st := range stmts {
			st.Close()
		}
	}()
	windowStart := sql.NullString{String: p.WindowStartedAt, Valid: true}
	changedSince := sql.NullString{String: p.ChangedSince, Valid: true}
	var buf bytes.Buffer
	for _, item := range p.Items {
		id, err := RecordID(item, p.Endpoint)
		if err != nil {
			return counts, err
		}
		obj := item.(*pyjson.Object)
		// Hashed key-sorted, so the hash only changes with the content; stored in Centric's own
		// key order.
		pyjson.EncodeTo(&buf, obj, true)
		sum := sha256.Sum256(buf.Bytes())
		hash := hex.EncodeToString(sum[:])
		payload := func() string {
			pyjson.EncodeTo(&buf, obj, false)
			return buf.String()
		}
		var modified sql.NullString
		if v, ok := obj.Get("_modified_at"); ok {
			if s, ok := pyjson.Text(v); ok && strings.TrimSpace(s) != "" {
				modified = sql.NullString{String: strings.TrimSpace(s), Valid: true}
			}
		}

		var old struct {
			modified sql.NullString
			hash     string
			deleted  bool
			lastRun  string
		}
		err = stmts["select"].QueryRow(p.Endpoint, id).Scan(&old.modified, &old.hash, &old.deleted, &old.lastRun)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return counts, err
		}
		if p.WindowStartedAt != "" && (!exists || old.lastRun != p.RunID) && CompareModified(modified, windowStart) < 0 {
			counts.LateFinds++
		}
		if p.ChangedSince != "" && CompareModified(modified, changedSince) >= 0 {
			counts.Concurrent++
		}

		newer := 0
		if exists {
			newer = CompareModified(modified, old.modified)
		}
		switch {
		case !exists:
			_, err = stmts["insert"].Exec(p.Endpoint, id, modified, payload(), hash, p.Now, p.Now, p.Now, p.RunID, p.ChangeSeq)
			counts.Inserted++
		case newer >= 0 && old.hash != hash:
			if newer == 0 {
				if err := counts.silent(stmts["payload"], p.Endpoint, id, obj, p.SilentSample); err != nil {
					return counts, err
				}
			}
			_, err = stmts["update"].Exec(modified, payload(), hash, p.Now, p.Now, p.RunID, p.ChangeSeq, p.Endpoint, id)
			counts.Updated++
		case old.deleted:
			_, err = stmts["restore"].Exec(p.Now, p.Now, p.RunID, p.ChangeSeq, p.Endpoint, id)
		default:
			_, err = stmts["seen"].Exec(p.Now, p.RunID, p.Endpoint, id)
			counts.Unchanged++
		}
		if err != nil {
			return counts, err
		}
		if exists && old.deleted {
			counts.Restored++
		}
	}
	return counts, nil
}

func prepare(tx *sql.Tx, queries map[string]string) (map[string]*sql.Stmt, error) {
	stmts := map[string]*sql.Stmt{}
	for name, q := range queries {
		st, err := tx.Prepare(q)
		if err != nil {
			for _, s := range stmts {
				s.Close()
			}
			return nil, err
		}
		stmts[name] = st
	}
	return stmts, nil
}

// silent counts a silent change and the top-level fields that differ from the stored payload.
func (c *PageCounts) silent(stored *sql.Stmt, endpoint, id string, obj *pyjson.Object, sample int) error {
	c.Silent++
	if c.SilentSampled >= sample {
		return nil
	}
	var before string
	if err := stored.QueryRow(endpoint, id).Scan(&before); err != nil {
		return err
	}
	parsed, _ := pyjson.Parse([]byte(before))
	old, ok := parsed.(*pyjson.Object)
	if !ok {
		return nil // a stored payload that doesn't parse can't say which fields changed
	}
	c.SilentSampled++
	if c.SilentFields == nil {
		c.SilentFields = map[string]int{}
	}
	for _, k := range ChangedFields(old, obj) {
		c.SilentFields[k]++
	}
	return nil
}

// ChangedFields lists the top-level keys whose values differ between a and b (either may be
// nil), in b's order, then a's keys missing from b.
func ChangedFields(a, b *pyjson.Object) []string {
	var out []string
	var bufA, bufB bytes.Buffer
	enc := func(o *pyjson.Object, k string, buf *bytes.Buffer) ([]byte, bool) {
		if o == nil {
			return nil, false
		}
		v, ok := o.Get(k)
		if !ok {
			return nil, false
		}
		pyjson.EncodeTo(buf, v, true)
		return buf.Bytes(), true
	}
	seen := map[string]bool{}
	for _, o := range []*pyjson.Object{b, a} {
		if o == nil {
			continue
		}
		for _, k := range o.Keys {
			if seen[k] {
				continue
			}
			seen[k] = true
			va, okA := enc(a, k, &bufA)
			vb, okB := enc(b, k, &bufB)
			if okA != okB || !bytes.Equal(va, vb) {
				out = append(out, k)
			}
		}
	}
	return out
}

// RecordID returns a record's id as text, or a BadRecord error.
func RecordID(item pyjson.Value, endpoint string) (string, error) {
	obj, ok := item.(*pyjson.Object)
	if !ok {
		return "", &BadRecord{fmt.Sprintf("%s: Centric returned a record that is not an object", endpoint)}
	}
	v, _ := obj.Get("id")
	if _, isBool := v.(bool); !isBool {
		if s, ok := pyjson.Text(v); ok && strings.TrimSpace(s) != "" {
			return s, nil
		}
	}
	return "", &BadRecord{fmt.Sprintf("%s: Centric returned a record without a usable 'id'", endpoint)}
}

// SeenCount counts an endpoint's records stamped with runID.
func SeenCount(tx *sql.Tx, endpoint, runID string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM bronze_records WHERE endpoint = ? AND last_seen_run = ?`,
		endpoint, runID).Scan(&n)
	return n, err
}

// UnseenIDs lists active records a full window did not return.
func UnseenIDs(tx *sql.Tx, endpoint, runID string) ([]string, error) {
	rows, err := tx.Query(`SELECT id FROM bronze_records
		WHERE endpoint = ? AND deleted = 0 AND last_seen_run != ? ORDER BY id`, endpoint, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ActiveCount counts an endpoint's records not flagged deleted.
func ActiveCount(tx *sql.Tx, endpoint string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM bronze_records WHERE endpoint = ? AND deleted = 0`, endpoint).Scan(&n)
	return n, err
}

// MarkDeleted flags active records as deleted and returns how many it flagged.
func MarkDeleted(tx *sql.Tx, endpoint string, ids []string, now string, changeSeq int64) (int, error) {
	stmt, err := tx.Prepare(`UPDATE bronze_records SET deleted = 1, deleted_at = ?, updated_at = ?,
		change_seq = ? WHERE endpoint = ? AND id = ? AND deleted = 0`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	flagged := 0
	for _, id := range ids {
		res, err := stmt.Exec(now, now, changeSeq, endpoint, id)
		if err != nil {
			return flagged, err
		}
		n, _ := res.RowsAffected()
		flagged += int(n)
	}
	return flagged, nil
}

// CompareModified orders _modified_at values; a missing value sorts first. Values that both
// parse as times compare as times, others as text.
func CompareModified(a, b sql.NullString) int {
	switch {
	case a.Valid == b.Valid && a.String == b.String:
		return 0
	case !a.Valid:
		return -1
	case !b.Valid:
		return 1
	}
	ta, okA := ParseTime(a.String)
	tb, okB := ParseTime(b.String)
	if okA && okB {
		return ta.Compare(tb)
	}
	return strings.Compare(a.String, b.String)
}
