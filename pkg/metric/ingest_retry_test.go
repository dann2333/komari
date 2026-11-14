package metric

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func retryTestStore(t *testing.T, policy RollupPolicy) *Store {
	t.Helper()
	s, err := Open(context.Background(), SQLite(filepath.Join(t.TempDir(), "metric.db"), WithMaxOpenConns(1), WithRollupPolicy(policy)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMetric(context.Background(), Definition{Name: "retry", RetentionDays: 30}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func minuteOnly() RollupPolicy {
	return RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 30 * 24 * time.Hour}}, Compression: 30}
}

func persistedCountSum(t *testing.T, s *Store, interval time.Duration, entity string) (int64, float64) {
	t.Helper()
	var count sql.NullInt64
	var sum sql.NullFloat64
	q := fmt.Sprintf(`SELECT SUM(r.count), SUM(r.sum) FROM %s r JOIN %s d ON r.resolution_id=d.id JOIN %s x ON r.series_id=x.id WHERE d.resolution_milli=? AND (?='' OR x.entity_id=?)`, s.tables.rollups, s.tables.resolutions, s.tables.series)
	if err := s.db.QueryRow(q, interval.Milliseconds(), entity, entity).Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	return count.Int64, sum.Float64
}

func TestWriteBatchRetriesDoNotAcceptFailedInput(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	old := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Hour)
	seed := Point{MetricName: "retry", EntityID: "seed", Timestamp: old, Value: 3}
	input := Point{MetricName: "retry", EntityID: "input", Timestamp: old, Value: 7}
	if err := s.Write(ctx, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_minute BEFORE INSERT ON metric_rollups BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Write(ctx, input); err == nil {
			t.Fatal("expected pre-acceptance failure")
		}
		if len(s.hot) != 1 {
			t.Fatalf("failed attempt changed hot state: %d", len(s.hot))
		}
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_minute`); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, input); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "input"); count != 1 || sum != 7 {
		t.Fatalf("input count=%d sum=%v", count, sum)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "seed"); count != 1 || sum != 3 {
		t.Fatalf("seed count=%d sum=%v", count, sum)
	}
}

func TestMinuteCommitThenCoarseFailureDoesNotAcceptRetry(t *testing.T) {
	ctx := context.Background()
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 10 * time.Hour}, {Interval: 5 * time.Minute, Retention: 30 * 24 * time.Hour}}, Compression: 30}
	s := retryTestStore(t, policy)
	old := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "minute", Timestamp: old, Value: 4}); err != nil {
		t.Fatal(err)
	}
	parent := newRollupBucket(30)
	parent.tagsJSON = "{}"
	parent.labelsJSON = "{}"
	parent.addPoint(3, old.UnixMilli())
	s.addCoarseChildren("retry", 5*time.Minute, nil, map[rollupKey]*rollupBucket{{entityID: "parent", bucket: old.UnixMilli()}: parent})
	if _, err := s.Flush(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "minute"); count != 1 || sum != 4 {
		t.Fatalf("minute count=%d sum=%v", count, sum)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_coarse BEFORE INSERT ON metric_rollups WHEN NEW.resolution_id IN (SELECT id FROM metric_resolutions WHERE resolution_milli=300000) BEGIN SELECT RAISE(ABORT,'coarse'); END`); err != nil {
		t.Fatal(err)
	}
	incoming := Point{MetricName: "retry", EntityID: "incoming", Timestamp: old, Value: 7}
	for i := 0; i < 3; i++ {
		if err := s.Write(ctx, incoming); err == nil {
			t.Fatal("expected coarse failure")
		}
		if len(s.hot) != 0 {
			t.Fatal("failed input was staged")
		}
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_coarse`); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, incoming); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "incoming"); count != 1 || sum != 7 {
		t.Fatalf("incoming count=%d sum=%v", count, sum)
	}
	if count, sum := persistedCountSum(t, s, 5*time.Minute, "parent"); count != 1 || sum != 3 {
		t.Fatalf("parent count=%d sum=%v", count, sum)
	}
}

func TestBoundedFlushFailureRetainsOnlyUncommittedChunks(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	old := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	points := make([]Point, 140)
	for i := range points {
		points[i] = Point{MetricName: "retry", EntityID: fmt.Sprintf("node-%03d", i), Timestamp: old, Value: 7}
	}
	if err := s.WriteBatch(ctx, points); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_after_first_chunk BEFORE INSERT ON metric_rollups WHEN (SELECT COUNT(*) FROM metric_rollups)>=60 BEGIN SELECT RAISE(ABORT,'second chunk'); END`); err != nil {
		t.Fatal(err)
	}
	n, err := s.Flush(ctx, time.Now())
	if err == nil || n != 60 {
		t.Fatalf("first flush n=%d err=%v", n, err)
	}
	if len(s.hot) != 80 {
		t.Fatalf("retained hot=%d want 80", len(s.hot))
	}
	if count, sum := persistedCountSum(t, s, time.Minute, ""); count != 60 || sum != 420 {
		t.Fatalf("committed count=%d sum=%v", count, sum)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_after_first_chunk`); err != nil {
		t.Fatal(err)
	}
	if n, err = s.Flush(ctx, time.Now()); err != nil || n != 80 {
		t.Fatalf("retry n=%d err=%v", n, err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, ""); count != 140 || sum != 980 {
		t.Fatalf("final count=%d sum=%v", count, sum)
	}
}

func TestStagedReplacementHidesPersistedOldLabels(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	now := time.Now().UTC()
	at := now.Truncate(time.Minute).Add(-2 * time.Minute)
	p := Point{MetricName: "retry", EntityID: "n1", Timestamp: at, Value: 7, Labels: map[string]string{"v": "old"}}
	if err := s.Write(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	p.Value = 9
	p.Labels = map[string]string{"v": "new"}
	if err := s.Write(ctx, p); err != nil {
		t.Fatal(err)
	}
	q := Query{MetricName: "retry", EntityID: "n1", Start: at.Add(-time.Minute), End: now}
	got, err := s.Series(ctx, AggregateQuery{Query: q, Aggregation: AggSum, Interval: time.Minute}, now)
	if err != nil || len(got) != 1 || got[0].Count != 1 || got[0].Value != 9 {
		t.Fatalf("series=%#v err=%v", got, err)
	}
	stats, err := s.Stats(ctx, q)
	if err != nil || stats.Count != 1 || stats.Sum != 9 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}
	latest, ok, err := s.LatestBefore(ctx, "retry", "n1", now)
	if err != nil || !ok || latest.Value != 9 || latest.Labels["v"] != "new" {
		t.Fatalf("latest=%#v ok=%v err=%v", latest, ok, err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "n1"); count != 1 || sum != 9 {
		t.Fatalf("persisted count=%d sum=%v", count, sum)
	}
}

func TestLateDataUsesRetainedTiersAndAlignedCutoff(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute).Add(30 * time.Second)
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 10 * time.Hour}, {Interval: 5 * time.Minute, Retention: 50 * time.Hour}, {Interval: time.Hour, Retention: 600 * time.Hour}, {Interval: 24 * time.Hour, Retention: 100 * 365 * 24 * time.Hour}}, Compression: 30}
	s := retryTestStore(t, policy)
	old := now.Add(-55 * time.Hour)
	cutoff := time.UnixMilli(bucketStartMillis(now.Add(-10*time.Hour).UnixMilli(), time.Minute.Milliseconds())).UTC()
	inputs := []Point{{MetricName: "retry", EntityID: "old", Timestamp: old, Value: 7}, {MetricName: "retry", EntityID: "cutoff", Timestamp: cutoff, Value: 11}, {MetricName: "retry", EntityID: "expired", Timestamp: now.Add(-40 * 24 * time.Hour), Value: 99}}
	if err := s.WriteBatch(ctx, inputs); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CleanupExpired(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	if count, _ := persistedCountSum(t, s, time.Minute, "old"); count != 0 {
		t.Fatalf("expired minute resurrected: %d", count)
	}
	if count, _ := persistedCountSum(t, s, 5*time.Minute, "old"); count != 0 {
		t.Fatalf("expired five minute resurrected: %d", count)
	}
	for _, tier := range []time.Duration{time.Hour, 24 * time.Hour} {
		if count, sum := persistedCountSum(t, s, tier, "old"); count != 1 || sum != 7 {
			t.Fatalf("tier %v count=%d sum=%v", tier, count, sum)
		}
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "cutoff"); count != 1 || sum != 11 {
		t.Fatalf("straddling cutoff count=%d sum=%v", count, sum)
	}
	for _, tier := range []time.Duration{time.Minute, 5 * time.Minute, time.Hour, 24 * time.Hour} {
		if count, _ := persistedCountSum(t, s, tier, "expired"); count != 0 {
			t.Fatalf("fully expired tier %v count=%d", tier, count)
		}
	}
}

func TestShortFineRetentionWaitsForRawReplacements(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Truncate(time.Minute).Add(-4 * time.Minute)
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 2 * time.Minute}, {Interval: 5 * time.Minute, Retention: time.Hour}}, Compression: 30}
	s := retryTestStore(t, policy)
	p := Point{MetricName: "retry", EntityID: "n1", Timestamp: old, Value: 7, Labels: map[string]string{"v": "old"}}
	if err := s.Write(ctx, p); err != nil {
		t.Fatal(err)
	}
	p.Value = 9
	p.Labels = map[string]string{"v": "new"}
	if err := s.Write(ctx, p); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Flush(ctx, now); err != nil || n != 0 {
		t.Fatalf("premature flush n=%d err=%v", n, err)
	}
	if _, err := s.Flush(ctx, now.Add(12*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, 5*time.Minute, "n1"); count != 1 || sum != 9 {
		t.Fatalf("coarse count=%d sum=%v", count, sum)
	}
}

func TestExpiredPendingParentAndLateDeltaBothSurvive(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	old := now.Add(-3 * time.Hour)
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: time.Hour}, {Interval: 5 * time.Minute, Retention: 2 * time.Hour}, {Interval: time.Hour, Retention: 20 * time.Hour}, {Interval: 24 * time.Hour, Retention: 60 * 24 * time.Hour}}, Compression: 30}
	s := retryTestStore(t, policy)
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "n1", Timestamp: old, Value: 7}); err != nil {
		t.Fatal(err)
	}
	tagsHash, tagsJSON, err := tagsFingerprint(nil)
	if err != nil {
		t.Fatal(err)
	}
	labelsHash, labelsJSON, err := tagsFingerprint(nil)
	if err != nil {
		t.Fatal(err)
	}
	b := newRollupBucket(30)
	b.tagsHash, b.tagsJSON = tagsHash, tagsJSON
	b.labelsHash, b.labelsJSON = labelsHash, labelsJSON
	b.addPoint(3, old.UnixMilli())
	key := rollupKey{entityID: "n1", tagsHash: tagsHash, labelsHash: labelsHash, bucket: old.UnixMilli()}
	s.addCoarseChildren("retry", 5*time.Minute, []time.Duration{time.Hour, 24 * time.Hour}, map[rollupKey]*rollupBucket{key: b})
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FlushCoarse(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FlushCoarse(ctx, now); err != nil {
		t.Fatal(err)
	}
	if count, _ := persistedCountSum(t, s, 5*time.Minute, "n1"); count != 0 {
		t.Fatalf("expired parent resurrected: %d", count)
	}
	if count, sum := persistedCountSum(t, s, time.Hour, "n1"); count != 2 || sum != 10 {
		t.Fatalf("hour count=%d sum=%v", count, sum)
	}
	// The daily parent may still be open; seal it using an explicit future clock.
	if _, err := s.FlushCoarse(ctx, now.Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, 24*time.Hour, "n1"); count != 2 || sum != 10 {
		t.Fatalf("day count=%d sum=%v", count, sum)
	}
}

func TestCloseFlushesAcceptedClosedAndOpenMinutes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "close.db")
	s, err := Open(ctx, SQLite(path, WithRollupPolicy(minuteOnly())))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMetric(ctx, Definition{Name: "retry", RetentionDays: 30}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.WriteBatch(ctx, []Point{{MetricName: "retry", EntityID: "old", Timestamp: now.Add(-time.Hour), Value: 7}, {MetricName: "retry", EntityID: "open", Timestamp: now, Value: 9}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, SQLite(path, WithRollupPolicy(minuteOnly())))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if count, sum := persistedCountSum(t, reopened, time.Minute, ""); count != 2 || sum != 16 {
		t.Fatalf("closed count=%d sum=%v", count, sum)
	}
}

func TestFlushReleasesLocksBetweenChunks(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	now := time.Now().UTC()
	old := now.Add(-time.Hour)
	points := make([]Point, 120)
	for i := range points {
		points[i] = Point{MetricName: "retry", EntityID: fmt.Sprintf("lock-%03d", i), Timestamp: old, Value: 1}
	}
	if err := s.WriteBatch(ctx, points); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	first, second := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var one, two sync.Once
	defer one.Do(func() { close(releaseFirst) })
	defer two.Do(func() { close(releaseSecond) })
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		return raw.(*sqlite3.SQLiteConn).RegisterFunc("test_flush_gate", func() int {
			n := calls.Add(1)
			if n == 1 {
				close(first)
				<-releaseFirst
			}
			if n == 61 {
				close(second)
				<-releaseSecond
			}
			return 1
		}, false)
	})
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER gate_flush BEFORE INSERT ON metric_rollups BEGIN SELECT test_flush_gate(); END`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.Flush(ctx, now); done <- err }()
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk did not start")
	}
	writerEntered, readerEntered := make(chan struct{}), make(chan struct{})
	go func() { s.ingestMu.Lock(); close(writerEntered); s.ingestMu.Unlock() }()
	go func() { s.rollupViewMu.RLock(); close(readerEntered); s.rollupViewMu.RUnlock() }()
	time.Sleep(20 * time.Millisecond)
	one.Do(func() { close(releaseFirst) })
	select {
	case <-writerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter between chunks")
	}
	select {
	case <-readerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not enter between chunks")
	}
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("second chunk did not start")
	}
	two.Do(func() { close(releaseSecond) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledWaitDoesNotAcceptBatch(t *testing.T) {
	s := retryTestStore(t, minuteOnly())
	s.ingestMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Write(ctx, Point{MetricName: "retry", EntityID: "n1", Timestamp: time.Now(), Value: 1})
	}()
	cancel()
	s.ingestMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if len(s.hot) != 0 {
		t.Fatal("canceled batch was accepted")
	}
}

func TestConcurrentWriteFlushAndReplacementQueries(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	now := time.Now().UTC()
	at := now.Truncate(time.Minute).Add(-2 * time.Minute)
	p := Point{MetricName: "retry", EntityID: "n1", Timestamp: at, Value: 1}
	if err := s.Write(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 2; i <= 100; i++ {
			p.Value = float64(i)
			if err := s.Write(ctx, p); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := s.Flush(ctx, now); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			got, err := s.Series(ctx, AggregateQuery{Query: Query{MetricName: "retry", EntityID: "n1", Start: at.Add(-time.Minute), End: now}, Aggregation: AggSum, Interval: time.Minute}, now)
			if err != nil {
				errCh <- err
				return
			}
			if len(got) != 1 || got[0].Count != 1 || got[0].Value < 1 || got[0].Value > 100 {
				errCh <- fmt.Errorf("inconsistent query: %#v", got)
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "n1"); count != 1 || sum != 100 {
		t.Fatalf("final count=%d sum=%v", count, sum)
	}
}

func TestReplacementKeepsDifferentTagsIndependent(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	now := time.Now().UTC()
	at := now.Truncate(time.Minute).Add(-2 * time.Minute)
	points := []Point{
		{MetricName: "retry", EntityID: "n1", Timestamp: at, Value: 7, Tags: map[string]string{"device": "a"}, Labels: map[string]string{"v": "old"}},
		{MetricName: "retry", EntityID: "n1", Timestamp: at, Value: 5, Tags: map[string]string{"device": "b"}},
	}
	if err := s.WriteBatch(ctx, points); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	points[0].Value = 9
	points[0].Labels = map[string]string{"v": "new"}
	if err := s.Write(ctx, points[0]); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		tag  string
		want float64
	}{{"a", 9}, {"b", 5}} {
		got, err := s.Series(ctx, AggregateQuery{Query: Query{MetricName: "retry", EntityID: "n1", Tags: map[string]string{"device": test.tag}, Start: at.Add(-time.Minute), End: now}, Aggregation: AggSum, Interval: time.Minute}, now)
		if err != nil || len(got) != 1 || got[0].Count != 1 || got[0].Value != test.want {
			t.Fatalf("tag %s: %#v, err=%v", test.tag, got, err)
		}
	}
}

func TestLateMinuteSurvivesFineTierExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 10 * time.Hour}, {Interval: 5 * time.Minute, Retention: 50 * time.Hour}, {Interval: time.Hour, Retention: 600 * time.Hour}}, Compression: 30}
	s := retryTestStore(t, policy)
	at := now.Add(-2 * time.Hour)
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "late", Timestamp: at, Value: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err != nil {
		t.Fatal(err)
	}
	for _, tier := range []time.Duration{time.Minute, 5 * time.Minute, time.Hour} {
		if count, sum := persistedCountSum(t, s, tier, "late"); count != 1 || sum != 7 {
			t.Fatalf("tier %s count=%d sum=%v", tier, count, sum)
		}
	}
	if _, err := s.CleanupExpired(ctx, now.Add(11*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if count, _ := persistedCountSum(t, s, time.Minute, "late"); count != 0 {
		t.Fatalf("minute not expired: %d", count)
	}
	if count, sum := persistedCountSum(t, s, 5*time.Minute, "late"); count != 1 || sum != 7 {
		t.Fatalf("coarse after expiry count=%d sum=%v", count, sum)
	}
}

type commitFaultDriver struct {
	base   sqlite3.SQLiteDriver
	action atomic.Int32 // 1: commit then lose response; 2: roll back then lose response
}

func (d *commitFaultDriver) Open(dsn string) (driver.Conn, error) {
	c, err := d.base.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &commitFaultConn{Conn: c, fault: d}, nil
}

type commitFaultConn struct {
	driver.Conn
	fault *commitFaultDriver
}

func (c *commitFaultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &commitFaultTx{Tx: tx, fault: c.fault}, nil
}
func (c *commitFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *commitFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type commitFaultTx struct {
	driver.Tx
	fault *commitFaultDriver
}

func (t *commitFaultTx) Commit() error {
	action := t.fault.action.Swap(0)
	if action == 2 {
		if err := t.Tx.Rollback(); err != nil {
			return err
		}
		return errors.New("lost commit response after rollback")
	}
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	if action == 1 {
		return errors.New("lost commit response after commit")
	}
	return nil
}

func commitFaultStore(t *testing.T, policy RollupPolicy) (*Store, *commitFaultDriver) {
	t.Helper()
	fault := &commitFaultDriver{}
	name := fmt.Sprintf("metric-commit-fault-%d", time.Now().UnixNano())
	sql.Register(name, fault)
	db, err := sql.Open(name, filepath.Join(t.TempDir(), "fault.db"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), SQLite("", WithDB(db), WithMaxOpenConns(1), WithRollupPolicy(policy)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMetric(context.Background(), Definition{Name: "retry", RetentionDays: 30}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = db.Close() })
	return s, fault
}

func TestAmbiguousMinuteCommitRetriesExactlyOnce(t *testing.T) {
	for _, action := range []int32{1, 2} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			ctx := context.Background()
			s, fault := commitFaultStore(t, minuteOnly())
			now := time.Now().UTC()
			old := now.Add(-time.Hour)
			if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "seed", Timestamp: old, Value: 7}); err != nil {
				t.Fatal(err)
			}
			fault.action.Store(action)
			if _, err := s.Flush(ctx, now); err == nil {
				t.Fatal("expected ambiguous commit")
			}
			_, err := s.Series(ctx, AggregateQuery{Query: Query{MetricName: "retry", EntityID: "seed", Start: old.Add(-time.Minute), End: now}, Aggregation: AggSum, Interval: time.Minute}, now)
			if !errors.Is(err, ErrFlushCommitUncertain) {
				t.Fatalf("query during uncertainty: %v", err)
			}
			if _, err := s.CleanupExpired(ctx, now); !errors.Is(err, ErrFlushCommitUncertain) {
				t.Fatalf("cleanup during uncertainty: %v", err)
			}
			// Lose the response to the recovery transaction too; a third attempt
			// must still use the same marker and not merge the original delta again.
			fault.action.Store(1)
			if _, err := s.Flush(ctx, now); err == nil {
				t.Fatal("expected ambiguous recovery commit")
			}
			if _, err := s.Flush(ctx, now); err != nil {
				t.Fatal(err)
			}
			if count, sum := persistedCountSum(t, s, time.Minute, "seed"); count != 1 || sum != 7 {
				t.Fatalf("count=%d sum=%v", count, sum)
			}
		})
	}
}

func TestAmbiguousCoarseCommitRetriesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Truncate(time.Minute).Add(-time.Hour)
	policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 10 * time.Hour}, {Interval: 5 * time.Minute, Retention: 30 * 24 * time.Hour}}, Compression: 30}
	s, fault := commitFaultStore(t, policy)
	b := newRollupBucket(30)
	b.tagsJSON = "{}"
	b.labelsJSON = "{}"
	b.addPoint(3, old.UnixMilli())
	s.addCoarseChildren("retry", 5*time.Minute, nil, map[rollupKey]*rollupBucket{{entityID: "parent", bucket: old.UnixMilli()}: b})
	fault.action.Store(1)
	if _, err := s.FlushCoarse(ctx, now); err == nil {
		t.Fatal("expected ambiguous coarse commit")
	}
	if _, err := s.FlushCoarse(ctx, now); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, 5*time.Minute, "parent"); count != 1 || sum != 3 {
		t.Fatalf("coarse count=%d sum=%v", count, sum)
	}
}

func TestFailedFlushUsesCurrentRetentionOnRetry(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	now := time.Now().UTC().Truncate(time.Minute)
	if _, err := s.SetMetricRetention(ctx, "retry", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "old", Timestamp: now.Add(-23 * time.Hour), Value: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_retention_retry BEFORE INSERT ON metric_rollups BEGIN SELECT RAISE(ABORT,'failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, now); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_retention_retry`); err != nil {
		t.Fatal(err)
	}
	later := now.Add(2 * time.Hour)
	if _, err := s.CleanupExpired(ctx, later); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, later); err != nil {
		t.Fatal(err)
	}
	if count, _ := persistedCountSum(t, s, time.Minute, "old"); count != 0 {
		t.Fatalf("expired bucket resurrected: %d", count)
	}
}

func TestCloseCanRetryFailedFlush(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "close-retry.db")
	s, err := Open(ctx, SQLite(path, WithRollupPolicy(minuteOnly())))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateMetric(ctx, Definition{Name: "retry", RetentionDays: 30}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "n1", Timestamp: time.Now(), Value: 7}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(ctx, `CREATE TRIGGER fail_close BEFORE INSERT ON metric_rollups BEGIN SELECT RAISE(ABORT,'close failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err == nil {
		t.Fatal("expected close flush failure")
	}
	if _, err := s.ExecContext(ctx, `DROP TRIGGER fail_close`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, SQLite(path, WithRollupPolicy(minuteOnly())))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if count, sum := persistedCountSum(t, reopened, time.Minute, "n1"); count != 1 || sum != 7 {
		t.Fatalf("count=%d sum=%v", count, sum)
	}
}

func TestAmbiguousRollbackUsesRetentionAtRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	later := now.Add(2 * time.Hour)
	t.Run("minute", func(t *testing.T) {
		s, fault := commitFaultStore(t, minuteOnly())
		if _, err := s.SetMetricRetention(ctx, "retry", 1); err != nil {
			t.Fatal(err)
		}
		if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "old", Timestamp: now.Add(-23 * time.Hour), Value: 7}); err != nil {
			t.Fatal(err)
		}
		fault.action.Store(2)
		if _, err := s.Flush(ctx, now); err == nil {
			t.Fatal("expected ambiguous rollback")
		}
		s.retentionMu.RLock()
		s.ingestMu.Lock()
		s.rollupViewMu.Lock()
		_, err := s.flushClosedHotRollupsUnderView(ctx, later)
		s.rollupViewMu.Unlock()
		s.ingestMu.Unlock()
		s.retentionMu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
		if count, _ := persistedCountSum(t, s, time.Minute, "old"); count != 0 {
			t.Fatalf("expired minute resurrected: %d", count)
		}
	})
	t.Run("coarse", func(t *testing.T) {
		policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: time.Hour}, {Interval: 5 * time.Minute, Retention: 2 * time.Hour}, {Interval: time.Hour, Retention: 30 * 24 * time.Hour}}, Compression: 30}
		s, fault := commitFaultStore(t, policy)
		old := now.Add(-90 * time.Minute)
		b := newRollupBucket(30)
		b.tagsJSON = "{}"
		b.labelsJSON = "{}"
		b.addPoint(7, old.UnixMilli())
		s.addCoarseChildren("retry", 5*time.Minute, []time.Duration{time.Hour}, map[rollupKey]*rollupBucket{{entityID: "old", bucket: old.UnixMilli()}: b})
		fault.action.Store(2)
		if _, err := s.FlushCoarse(ctx, now); err == nil {
			t.Fatal("expected ambiguous rollback")
		}
		s.retentionMu.RLock()
		s.ingestMu.Lock()
		s.rollupViewMu.Lock()
		_, err := s.flushClosedCoarseRollupsUnderView(ctx, later)
		s.rollupViewMu.Unlock()
		s.ingestMu.Unlock()
		s.retentionMu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.FlushCoarse(ctx, later); err != nil {
			t.Fatal(err)
		}
		if count, _ := persistedCountSum(t, s, 5*time.Minute, "old"); count != 0 {
			t.Fatalf("expired coarse bucket resurrected: %d", count)
		}
		if count, sum := persistedCountSum(t, s, time.Hour, "old"); count != 1 || sum != 7 {
			t.Fatalf("retained hourly count=%d sum=%v", count, sum)
		}
	})
}

func TestReplacementLabelsStayInOneFlushChunk(t *testing.T) {
	s := retryTestStore(t, minuteOnly())
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Minute).UnixMilli()
	s.hotMu.Lock()
	for i := 0; i < hotRollupFlushBatchSize+10; i++ {
		key := hotRollupKey{metricName: "retry", entityID: fmt.Sprintf("other-%d", i), bucket: at}
		s.hot[key] = newRollupBucket(30)
	}
	old := hotRollupKey{metricName: "retry", entityID: "same", labelsHash: "old", bucket: at}
	newKey := hotRollupKey{metricName: "retry", entityID: "same", labelsHash: "new", bucket: at}
	s.hot[old] = newRollupBucket(30)
	s.hot[newKey] = newRollupBucket(30)
	s.addHotReplacementLocked(old)
	s.addHotReplacementLocked(newKey)
	s.hotMu.Unlock()
	for {
		chunk := s.takeClosedHotRollups(at + time.Minute.Milliseconds())
		if len(chunk) == 0 {
			break
		}
		var sawOld, sawNew bool
		for _, item := range chunk {
			sawOld = sawOld || item.key == old
			sawNew = sawNew || item.key == newKey
		}
		if sawOld != sawNew {
			t.Fatal("label replacement was split across transactions")
		}
	}
}

func TestTimeoutBeforeAcceptanceCanRetry(t *testing.T) {
	ctx := context.Background()
	s := retryTestStore(t, minuteOnly())
	old := time.Now().UTC().Add(-time.Hour)
	if err := s.Write(ctx, Point{MetricName: "retry", EntityID: "seed", Timestamp: old, Value: 3}); err != nil {
		t.Fatal(err)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.Raw(func(raw any) error {
		return raw.(*sqlite3.SQLiteConn).RegisterFunc("test_timeout", func() int { time.Sleep(100 * time.Millisecond); return 1 }, false)
	})
	_ = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER slow_flush BEFORE INSERT ON metric_rollups BEGIN SELECT test_timeout(); END`); err != nil {
		t.Fatal(err)
	}
	input := Point{MetricName: "retry", EntityID: "input", Timestamp: old, Value: 7}
	timed, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	err = s.Write(timed, input)
	cancel()
	if err == nil || !errors.Is(timed.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected timeout: %v", err)
	}
	if len(s.hot) != 1 {
		t.Fatalf("failed input was staged: %d", len(s.hot))
	}
	if _, err := s.db.Exec(`DROP TRIGGER slow_flush`); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, input); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if count, sum := persistedCountSum(t, s, time.Minute, "input"); count != 1 || sum != 7 {
		t.Fatalf("count=%d sum=%v", count, sum)
	}
}

func TestExpiredReplacementUpdatesPendingParent(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprint(failCommit), func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Minute)
			policy := RollupPolicy{Tiers: []RollupTier{{Interval: time.Minute, Retention: 10 * time.Hour}, {Interval: 5 * time.Minute, Retention: 50 * time.Hour}, {Interval: time.Hour, Retention: 600 * time.Hour}, {Interval: 24 * time.Hour, Retention: 30 * 24 * time.Hour}}, Compression: 30}
			s, fault := commitFaultStore(t, policy)
			base := now.Add(-11 * time.Hour).Truncate(5 * time.Minute)
			at := base.Add(time.Minute + 10*time.Second)
			first := base.Add(2 * time.Minute)
			p := Point{MetricName: "retry", EntityID: "n1", Timestamp: at, Value: 7, Labels: map[string]string{"v": "old"}}
			writeRollupPointsAt(t, ctx, s, first, p)
			p.Value = 9
			p.Labels = map[string]string{"v": "new"}
			prepared, err := prepareMetricPoints([]Point{p})
			if err != nil {
				t.Fatal(err)
			}
			rebuild := s.writeRawPointsAt(prepared, first)
			s.stagePreparedHotRollups(prepared, rebuild)
			if _, err := s.CleanupExpired(ctx, now); err != nil {
				t.Fatal(err)
			}
			if failCommit {
				fault.action.Store(1)
				if _, err := s.Flush(ctx, now); err == nil {
					t.Fatal("expected uncertain commit")
				}
			}
			if _, err := s.Flush(ctx, now); err != nil {
				t.Fatal(err)
			}
			if _, err := s.FlushCoarse(ctx, now); err != nil {
				t.Fatal(err)
			}
			if count, _ := persistedCountSum(t, s, time.Minute, "n1"); count != 0 {
				t.Fatalf("expired minute returned: %d", count)
			}
			for _, tier := range []time.Duration{5 * time.Minute, time.Hour, 24 * time.Hour} {
				if count, sum := persistedCountSum(t, s, tier, "n1"); count != 1 || sum != 9 {
					t.Fatalf("tier %s count=%d sum=%v", tier, count, sum)
				}
			}
		})
	}
}
