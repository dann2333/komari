package metric

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// A marker is committed in the same transaction as the rollups. Retrying its
// insertion lets the database decide whether an earlier ambiguous COMMIT won;
// a conflicting insert waits for any still-running transaction to finish.
type flushCommitError struct{ err error }

func (e *flushCommitError) Error() string {
	return "metric rollup commit outcome is uncertain: " + e.err.Error()
}
func (e *flushCommitError) Unwrap() error { return e.err }

func newFlushToken() string { return "flush:" + uuid.NewString() }

func (s *Store) beginFlushTx(ctx context.Context, token string) (*sql.Tx, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	if s.confirmedFlushToken != "" && s.confirmedFlushToken != token {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+s.tables.state+" WHERE state_key = "+s.dialect.placeholder(1), s.confirmedFlushToken); err != nil {
			_ = tx.Rollback()
			return nil, false, err
		}
	}
	columns := "(state_key, phase, upper_rowid, cursor_rowid, updated_at_milli)"
	values := "(" + s.dialect.placeholder(1) + ", " + s.dialect.placeholder(2) + ", " + s.dialect.placeholder(3) + ", " + s.dialect.placeholder(4) + ", " + s.dialect.placeholder(5) + ")"
	res, err := tx.ExecContext(ctx, s.insertIgnoreSQL(s.tables.state, columns, values), token, "rollup-flush", 0, 0, timeMillis(time.Now()))
	if err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	return tx, n == 0, nil
}

func (s *Store) commitFlushTx(tx *sql.Tx, token string) error {
	if err := tx.Commit(); err != nil {
		return &flushCommitError{err: err}
	}
	s.confirmedFlushToken = token
	return nil
}

func (s *Store) setFlushUncertain(value bool) {
	s.mu.Lock()
	s.flushUncertain = value
	s.mu.Unlock()
}

// recoverUncertainFlush is the only path allowed to change state after an
// ambiguous rollup commit. Normal reads fail until the marker resolves it.
func (s *Store) recoverUncertainFlush(ctx context.Context) error {
	s.mu.RLock()
	uncertain := s.flushUncertain
	closed := s.closed
	s.mu.RUnlock()
	if !uncertain || closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.retentionMu.RLock()
	defer s.retentionMu.RUnlock()
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	s.rollupViewMu.Lock()
	defer s.rollupViewMu.Unlock()
	s.mu.RLock()
	uncertain = s.flushUncertain
	s.mu.RUnlock()
	if !uncertain {
		return nil
	}
	if len(s.retryHot) != 0 {
		_, err := s.flushClosedHotRollupsUnderView(ctx, time.Now().UTC())
		return err
	}
	if len(s.retryCoarse) != 0 {
		_, err := s.flushClosedCoarseRollupsUnderView(ctx, time.Now().UTC())
		return err
	}
	return fmt.Errorf("%w: missing retry batch", ErrFlushCommitUncertain)
}
