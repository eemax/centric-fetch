package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// MaintainResult is what upkeep did.
type MaintainResult struct {
	Integrity  string   `json:"integrity"`
	Vacuumed   bool     `json:"vacuumed"`
	SizeBefore int64    `json:"size_before"`
	SizeAfter  int64    `json:"size_after"`
	Seconds    float64  `json:"seconds"`
	Notes      []string `json:"notes,omitempty"`
}

// Maintain runs upkeep under the writer lock: quick integrity check, VACUUM (only when
// there's room for the temporary copy, about twice the database), PRAGMA optimize and a WAL
// checkpoint. A failed integrity check returns an error and skips the rest.
func (db *DB) Maintain() (*MaintainResult, error) {
	start := time.Now()
	res := &MaintainResult{SizeBefore: fileSize(db.Path)}
	rows, err := db.Query("PRAGMA quick_check")
	if err != nil {
		return nil, err
	}
	var lines []string
	for rows.Next() {
		var line string
		rows.Scan(&line)
		if len(lines) < 5 {
			lines = append(lines, line)
		}
	}
	rows.Close()
	res.Integrity = strings.Join(lines, "; ")
	if res.Integrity != "ok" {
		return res, fmt.Errorf("integrity check failed: %s", res.Integrity)
	}
	var st unix.Statfs_t
	needed := uint64(float64(res.SizeBefore) * 2.1)
	if err := unix.Statfs(filepath.Dir(db.Path), &st); err == nil && st.Bavail*uint64(st.Bsize) < needed {
		res.Notes = append(res.Notes, fmt.Sprintf("vacuum skipped: needs ~%d bytes free, have %d", needed, st.Bavail*uint64(st.Bsize)))
	} else {
		if _, err := db.Exec("VACUUM"); err != nil {
			return res, fmt.Errorf("vacuum: %w", err)
		}
		res.Vacuumed = true
	}
	if _, err := db.Exec("PRAGMA optimize"); err != nil {
		return res, err
	}
	var busy, logFrames, done int
	if err := db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &done); err != nil {
		return res, err
	}
	if busy != 0 {
		res.Notes = append(res.Notes, "checkpoint incomplete: a reader was active; the next one finishes it")
	}
	res.SizeAfter = fileSize(db.Path)
	res.Seconds = float64(int64(time.Since(start).Seconds()*10+0.5)) / 10
	return res, nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
