package store

import (
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
}

// Add sums o into c.
func (c *PageCounts) Add(o PageCounts) {
	c.Inserted += o.Inserted
	c.Updated += o.Updated
	c.Unchanged += o.Unchanged
	c.Restored += o.Restored
	c.LateFinds += o.LateFinds
	c.Concurrent += o.Concurrent
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
}

// ApplyPage upserts a page. A stored payload is only replaced by one with the same or a
// newer _modified_at, so overlapping or out-of-order pages never roll a record back. Every
// record seen is stamped with the run id, which full fetches use to find deletions.
func ApplyPage(tx *sql.Tx, p Page) (PageCounts, error) {
	var counts PageCounts
	sel, err := tx.Prepare(`SELECT modified_at, payload_sha256, deleted, last_seen_run
		FROM bronze_records WHERE endpoint = ? AND id = ?`)
	if err != nil {
		return counts, err
	}
	defer sel.Close()
	for _, item := range p.Items {
		id, err := RecordID(item, p.Endpoint)
		if err != nil {
			return counts, err
		}
		obj := item.(*pyjson.Object)
		// Stored in Centric's own key order; hashed key-sorted, so the hash only changes with
		// the content.
		payload := string(pyjson.Encode(obj, false))
		sum := sha256.Sum256(pyjson.Encode(obj, true))
		hash := hex.EncodeToString(sum[:])
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
		err = sel.QueryRow(p.Endpoint, id).Scan(&old.modified, &old.hash, &old.deleted, &old.lastRun)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return counts, err
		}
		if p.WindowStartedAt != "" && (!exists || old.lastRun != p.RunID) &&
			CompareModified(modified, sql.NullString{String: p.WindowStartedAt, Valid: true}) < 0 {
			counts.LateFinds++
		}
		if p.ChangedSince != "" && CompareModified(modified, sql.NullString{String: p.ChangedSince, Valid: true}) >= 0 {
			counts.Concurrent++
		}

		switch {
		case !exists:
			_, err = tx.Exec(`INSERT INTO bronze_records (endpoint, id, modified_at, payload,
				payload_sha256, deleted, deleted_at, first_seen_at, updated_at, last_seen_at,
				last_seen_run, change_seq) VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?, ?)`,
				p.Endpoint, id, modified, payload, hash, p.Now, p.Now, p.Now, p.RunID, p.ChangeSeq)
			counts.Inserted++
		case CompareModified(modified, old.modified) >= 0 && old.hash != hash:
			_, err = tx.Exec(`UPDATE bronze_records SET modified_at = ?, payload = ?,
				payload_sha256 = ?, deleted = 0, deleted_at = NULL, updated_at = ?,
				last_seen_at = ?, last_seen_run = ?, change_seq = ? WHERE endpoint = ? AND id = ?`,
				modified, payload, hash, p.Now, p.Now, p.RunID, p.ChangeSeq, p.Endpoint, id)
			counts.Updated++
		case old.deleted:
			_, err = tx.Exec(`UPDATE bronze_records SET deleted = 0, deleted_at = NULL,
				updated_at = ?, last_seen_at = ?, last_seen_run = ?, change_seq = ?
				WHERE endpoint = ? AND id = ?`,
				p.Now, p.Now, p.RunID, p.ChangeSeq, p.Endpoint, id)
		default:
			_, err = tx.Exec(`UPDATE bronze_records SET last_seen_at = ?, last_seen_run = ?
				WHERE endpoint = ? AND id = ?`, p.Now, p.RunID, p.Endpoint, id)
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
