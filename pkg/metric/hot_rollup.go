package metric

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"
)

// hotRollupKey identifies one active minute bucket. The dictionary dimensions
// remain compact strings; full tag and label JSON lives only on the bucket.
type hotRollupKey struct {
	metricName string
	entityID   string
	tagsHash   string
	labelsHash string
	bucket     int64
}

// Labels for one raw series and minute must move together when a raw sample
// changes label sets, so a coarse query cannot observe both versions.
type hotReplacementGroup struct {
	metricName, entityID, tagsHash string
	bucket                         int64
}

func (key hotRollupKey) replacementGroup() hotReplacementGroup {
	return hotReplacementGroup{key.metricName, key.entityID, key.tagsHash, key.bucket}
}

func (s *Store) addHotReplacementLocked(key hotRollupKey) {
	s.hotReplace[key] = struct{}{}
	if s.hotReplaceGroups == nil {
		s.hotReplaceGroups = make(map[hotReplacementGroup]map[hotRollupKey]struct{})
	}
	group := key.replacementGroup()
	if s.hotReplaceGroups[group] == nil {
		s.hotReplaceGroups[group] = make(map[hotRollupKey]struct{})
	}
	s.hotReplaceGroups[group][key] = struct{}{}
}

func (s *Store) removeHotReplacementLocked(key hotRollupKey) {
	delete(s.hotReplace, key)
	group := key.replacementGroup()
	delete(s.hotReplaceGroups[group], key)
	if len(s.hotReplaceGroups[group]) == 0 {
		delete(s.hotReplaceGroups, group)
	}
}

type hotRollup struct {
	key     hotRollupKey
	bucket  *rollupBucket
	replace bool
}

// Keep each transaction small enough that ingestion and rollup readers can
// run between chunks even when a backlog contains hundreds of thousands of rows.
const hotRollupFlushBatchSize = 60

func (s *Store) stagePreparedHotRollups(prepared []preparedMetricPoint, rebuild map[hotRollupKey]struct{}) {
	minuteMillis := time.Minute.Milliseconds()
	compression := s.cfg.RollupPolicy.compression()
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	for _, point := range prepared {
		key := hotRollupKey{metricName: point.metricName, entityID: point.entityID,
			tagsHash: point.tagsHash, labelsHash: point.labelsHash,
			bucket: bucketStartMillis(point.timestamp, minuteMillis)}
		bucket := s.hot[key]
		if bucket == nil {
			bucket = newRollupBucket(compression)
			bucket.tagsHash, bucket.tagsJSON = point.tagsHash, point.tagsJSON
			bucket.labelsHash, bucket.labelsJSON = point.labelsHash, point.labelsJSON
			s.hot[key] = bucket
			s.noteHotBucketLocked(key.bucket)
		}
		bucket.addPoint(point.value, point.timestamp)
	}
	if len(rebuild) > 0 {
		s.rebuildHotRollupsLocked(rebuild, compression)
	}
}

// writePreparedHotRollups is retained for internal callers that explicitly
// supply a clock and expect a flush after staging.
func (s *Store) writePreparedHotRollups(ctx context.Context, prepared []preparedMetricPoint, now time.Time, rebuild map[hotRollupKey]struct{}) error {
	s.rollupViewMu.Lock()
	defer s.rollupViewMu.Unlock()
	s.stagePreparedHotRollups(prepared, rebuild)
	if _, err := s.flushClosedHotRollupsUnderView(ctx, now); err != nil {
		return err
	}
	_, err := s.flushClosedCoarseRollupsUnderView(ctx, now)
	return err
}

func (s *Store) rebuildHotRollupsLocked(keys map[hotRollupKey]struct{}, compression float64) {
	s.rawMu.RLock()
	defer s.rawMu.RUnlock()
	minuteMillis := time.Minute.Milliseconds()
	for key := range keys {
		series := s.raw[rawSeriesKey{metricName: key.metricName, entityID: key.entityID, tagsHash: key.tagsHash}]
		bucket := newRollupBucket(compression)
		bucket.tagsHash = key.tagsHash
		bucket.labelsHash = key.labelsHash
		if series != nil {
			bucket.tagsJSON = series.tagsJSON
			if labelID, ok := series.labelIDs[key.labelsHash]; ok {
				bucket.labelsJSON = series.labelsJSON[labelID]
			}
			bucketEnd := key.bucket + minuteMillis
			decoder := newRawSampleDecoder(series.compressed)
			for sample, more := decoder.next(); more; sample, more = decoder.next() {
				if sample.timestamp < key.bucket {
					continue
				}
				if sample.timestamp >= bucketEnd {
					break
				}
				if series.labelHashes[sample.labelID] != key.labelsHash {
					continue
				}
				bucket.labelsJSON = series.labelsJSON[sample.labelID]
				bucket.addPoint(sample.value, sample.timestamp)
			}
			directStart := sort.Search(len(series.samples), func(i int) bool { return series.samples[i].timestamp >= key.bucket })
			directEnd := sort.Search(len(series.samples), func(i int) bool { return series.samples[i].timestamp >= bucketEnd })
			for _, sample := range series.samples[directStart:directEnd] {
				if series.labelHashes[sample.labelID] != key.labelsHash {
					continue
				}
				bucket.labelsJSON = series.labelsJSON[sample.labelID]
				bucket.addPoint(sample.value, sample.timestamp)
			}
		}
		s.hot[key] = bucket
		s.addHotReplacementLocked(key)
		s.noteHotBucketLocked(key.bucket)
	}
}

func (s *Store) flushClosedHotRollups(ctx context.Context, now time.Time) (int, error) {
	s.rollupViewMu.Lock()
	defer s.rollupViewMu.Unlock()
	return s.flushClosedHotRollupsUnderView(ctx, now)
}

func (s *Store) flushClosedHotRollupsUnderView(ctx context.Context, now time.Time) (int, error) {
	flushBefore := bucketStartMillis(now.UTC().UnixMilli(), time.Minute.Milliseconds())
	// Preserve exact replacements when a fine tier expires inside the raw window.
	if len(s.cfg.RollupPolicy.Tiers) > 1 && s.cfg.RollupPolicy.Tiers[0].Retention < rawMemoryRetention {
		flushBefore = bucketStartMillis(now.UTC().Add(-rawMemoryRetention).UnixMilli(), time.Minute.Milliseconds())
	}
	return s.flushHotRollupsUnderView(ctx, now, flushBefore)
}

func (s *Store) flushHotRollupsUnderView(ctx context.Context, now time.Time, before int64) (int, error) {
	closed := s.retryHot
	token := s.retryHotToken
	originalAt := now
	if len(closed) != 0 {
		originalAt = s.retryHotAt
		s.hotMu.Lock()
		for _, item := range closed {
			delete(s.hot, item.key)
			s.removeHotReplacementLocked(item.key)
		}
		s.hotOldest = 0
		s.hotMu.Unlock()
	} else {
		closed = s.takeClosedHotRollups(before)
		if len(closed) == 0 {
			return 0, nil
		}
		token = newFlushToken()
	}
	if err := s.persistHotRollups(ctx, closed, &now, originalAt, token); err != nil {
		s.restoreHotRollups(closed)
		var uncertain *flushCommitError
		if len(s.retryHot) != 0 || errors.As(err, &uncertain) {
			s.retryHot, s.retryHotToken, s.retryHotAt = closed, token, now
			s.setFlushUncertain(true)
		}
		return 0, err
	}
	s.retryHot, s.retryHotToken, s.retryHotAt = nil, "", time.Time{}
	s.setFlushUncertain(false)
	return len(closed), nil
}

func (s *Store) flushAllHotRollups(ctx context.Context) error {
	for {
		s.rollupViewMu.Lock()
		n, err := s.flushHotRollupsUnderView(ctx, time.Now().UTC(), math.MaxInt64)
		s.rollupViewMu.Unlock()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func (s *Store) takeClosedHotRollups(currentBucket int64) []hotRollup {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	if s.hotOldest >= currentBucket {
		return nil
	}
	closed := make([]hotRollup, 0, hotRollupFlushBatchSize)
	oldest := int64(math.MaxInt64)
	for key, bucket := range s.hot {
		if key.bucket >= currentBucket {
			oldest = min(oldest, key.bucket)
			continue
		}
		if len(closed) >= hotRollupFlushBatchSize {
			oldest = 0
			break
		}
		_, replace := s.hotReplace[key]
		if replace {
			group := s.hotReplaceGroups[key.replacementGroup()]
			if len(closed) != 0 && len(closed)+len(group) > hotRollupFlushBatchSize {
				oldest = 0
				break
			}
			for groupedKey := range group {
				closed = append(closed, hotRollup{key: groupedKey, bucket: s.hot[groupedKey], replace: true})
				delete(s.hot, groupedKey)
				s.removeHotReplacementLocked(groupedKey)
			}
			continue
		}
		closed = append(closed, hotRollup{key: key, bucket: bucket})
		delete(s.hot, key)
	}
	s.hotOldest = oldest
	return closed
}

// noteHotBucketLocked keeps hotOldest a lower bound after inserting a key.
// The caller must hold hotMu.
func (s *Store) noteHotBucketLocked(bucket int64) {
	if bucket < s.hotOldest {
		s.hotOldest = bucket
	}
}

func (s *Store) restoreHotRollups(closed []hotRollup) {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	for _, item := range closed {
		current, exists := s.hot[item.key]
		_, currentReplaces := s.hotReplace[item.key]
		if !exists {
			s.hot[item.key] = item.bucket
			s.noteHotBucketLocked(item.key.bucket)
			if item.replace {
				s.addHotReplacementLocked(item.key)
			}
			continue
		}
		if currentReplaces {
			continue
		}
		if item.replace {
			item.bucket.mergeStored(current)
			s.hot[item.key] = item.bucket
			s.addHotReplacementLocked(item.key)
			continue
		}
		current.mergeStored(item.bucket)
	}
}

// tierRetainsBucket matches CleanupExpired's bucket-aligned cutoff, including
// the one bucket that straddles the cutoff.
func tierRetainsBucket(tier RollupTier, bucket int64, now time.Time) bool {
	start := bucketStartMillis(bucket, tier.Interval.Milliseconds())
	cutoff := bucketStartMillis(now.UTC().Add(-tier.Retention).UnixMilli(), tier.Interval.Milliseconds())
	return start >= cutoff
}

func (s *Store) hasPendingMinuteParent(name string, policy RollupPolicy, key rollupKey) bool {
	if len(policy.Tiers) < 2 {
		return false
	}
	interval := policy.Tiers[1].Interval
	parent := coarseRollupKey{metricName: name, interval: interval, rollupKey: rollupKey{entityID: key.entityID, tagsHash: key.tagsHash, labelsHash: key.labelsHash, bucket: bucketStartMillis(key.bucket, interval.Milliseconds())}}
	s.coarseMu.RLock()
	_, ok := s.coarse[parent]
	s.coarseMu.RUnlock()
	return ok
}

func (s *Store) persistHotRollups(ctx context.Context, closed []hotRollup, now *time.Time, originalAt time.Time, token string) error {
	type metricBuckets struct {
		items  []hotRollup
		policy RollupPolicy
		minute map[rollupKey]*rollupBucket
	}
	byMetric := make(map[string]*metricBuckets)
	for _, item := range closed {
		buckets := byMetric[item.key.metricName]
		if buckets == nil {
			buckets = &metricBuckets{}
			byMetric[item.key.metricName] = buckets
		}
		buckets.items = append(buckets.items, item)
	}
	names := make([]string, 0, len(byMetric))
	for name := range byMetric {
		names = append(names, name)
	}
	sort.Strings(names)
	tx, alreadyCommitted, err := s.beginFlushTx(ctx, token)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if alreadyCommitted {
		*now = originalAt
	}
	for _, name := range names {
		buckets := byMetric[name]
		policy, err := s.metricRollupPolicyTx(ctx, name, tx)
		if err != nil {
			return err
		}
		buckets.policy = policy
		if len(policy.Tiers) == 0 {
			continue
		}
		if alreadyCommitted {
			// The first COMMIT succeeded even though its response was lost.
			// Reconstruct full children for the in-memory coarse cascade only.
			buckets.minute = make(map[rollupKey]*rollupBucket)
			for _, item := range buckets.items {
				key := rollupKey{entityID: item.key.entityID, tagsHash: item.key.tagsHash, labelsHash: item.key.labelsHash, bucket: item.key.bucket}
				if !tierRetainsBucket(policy.Tiers[0], key.bucket, *now) {
					if item.replace && s.hasPendingMinuteParent(name, policy, key) {
						buckets.minute[key] = item.bucket
					}
					continue
				}
				bucket, err := s.readRollupBucketTx(ctx, name, key, policy.Tiers[0].Interval, tx)
				if err != nil {
					return err
				}
				buckets.minute[key] = bucket
			}
			continue
		}
		merge := make(map[rollupKey]*rollupBucket)
		replace := make(map[rollupKey]*rollupBucket)
		late := make(map[time.Duration]map[rollupKey]*rollupBucket)
		buckets.minute = make(map[rollupKey]*rollupBucket)
		for _, item := range buckets.items {
			key := rollupKey{entityID: item.key.entityID, tagsHash: item.key.tagsHash, labelsHash: item.key.labelsHash, bucket: item.key.bucket}
			minuteRetained := tierRetainsBucket(policy.Tiers[0], key.bucket, *now)
			if !minuteRetained && item.replace && s.hasPendingMinuteParent(name, policy, key) {
				buckets.minute[key] = item.bucket
				continue
			}
			if minuteRetained {
				if item.replace {
					replace[key] = item.bucket
				} else {
					merge[key] = item.bucket
				}
				if item.replace || len(policy.Tiers) < 2 {
					continue
				}
				first := policy.Tiers[1].Interval
				parentStart := bucketStartMillis(key.bucket, first.Milliseconds())
				if parentStart+first.Milliseconds()+coarseRollupGrace.Milliseconds() > now.UnixMilli() {
					continue
				}
				if s.hasPendingMinuteParent(name, policy, key) {
					continue
				} // addMinuteParents updates this unsealed parent.
			}
			// A late delta whose normal parent already sealed, or whose minute
			// has expired, must survive in each still-retained coarse tier.
			if item.bucket == nil || item.bucket.count == 0 {
				continue
			}
			for _, tier := range policy.Tiers[1:] {
				if !tierRetainsBucket(tier, key.bucket, *now) {
					continue
				}
				parent := key
				parent.bucket = bucketStartMillis(key.bucket, tier.Interval.Milliseconds())
				group := late[tier.Interval]
				if group == nil {
					group = make(map[rollupKey]*rollupBucket)
					late[tier.Interval] = group
				}
				if group[parent] == nil {
					group[parent] = newRollupBucket(s.cfg.RollupPolicy.compression())
					group[parent].tagsHash, group[parent].tagsJSON = item.bucket.tagsHash, item.bucket.tagsJSON
					group[parent].labelsHash, group[parent].labelsJSON = item.bucket.labelsHash, item.bucket.labelsJSON
				}
				group[parent].mergeStored(item.bucket)
			}
		}
		if _, err := s.mergeRollupBucketsWithResultTx(ctx, name, policy.Tiers[0].Interval, merge, newRollupDictionaryCache(), tx, buckets.minute); err != nil {
			return err
		}
		if err := s.replaceMinuteRollupsTx(ctx, name, policy, replace, tx); err != nil {
			return err
		}
		for key, bucket := range replace {
			buckets.minute[key] = bucket
		}
		for _, tier := range policy.Tiers[1:] {
			if _, err := s.mergeRollupBucketsWithDictionaryTx(ctx, name, tier.Interval, late[tier.Interval], newRollupDictionaryCache(), tx); err != nil {
				return err
			}
		}
	}
	if err := s.commitFlushTx(tx, token); err != nil {
		return err
	}
	for _, name := range names {
		buckets := byMetric[name]
		s.addMinuteParents(name, buckets.policy, buckets.minute, *now)
	}
	return nil
}

func (s *Store) hotRollupRows(metricName, entityID string, tags map[string]string, start, end time.Time, needDigest bool) ([]storedRollup, error) {
	startMilli, endMilli := start.UTC().UnixMilli(), end.UTC().UnixMilli()
	s.hotMu.RLock()
	defer s.hotMu.RUnlock()
	out := make([]storedRollup, 0)
	for key, bucket := range s.hot {
		if key.metricName != metricName || key.bucket+time.Minute.Milliseconds() <= startMilli || key.bucket > endMilli || (entityID != "" && key.entityID != entityID) {
			continue
		}
		if bucket.count == 0 {
			continue
		}
		bucketTags, err := rollupTagsFromJSON(bucket.tagsJSON)
		if err != nil {
			return nil, err
		}
		matched := true
		for name, value := range tags {
			if bucketTags[name] != value {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		out = append(out, storedRollup{entityID: key.entityID, bucket: key.bucket, bucketData: bucket.clone(needDigest)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].bucket < out[j].bucket })
	return out, nil
}

// deleteHotRollups removes active minute buckets matched by the same series
// dimensions used by persisted deletes. A non-nil cutoff removes only buckets
// whose minute starts before it.
func (s *Store) deleteHotRollups(metricName, entityID string, tags map[string]string, cutoff *time.Time) (int64, error) {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()

	var cutoffMilli int64
	if cutoff != nil {
		cutoffMilli = cutoff.UTC().UnixMilli()
	}
	matched := make([]hotRollupKey, 0)
	for key, bucket := range s.hot {
		if metricName != "" && key.metricName != metricName {
			continue
		}
		if entityID != "" && key.entityID != entityID {
			continue
		}
		if cutoff != nil && key.bucket >= cutoffMilli {
			continue
		}
		if len(tags) > 0 {
			bucketTags, err := rollupTagsFromJSON(bucket.tagsJSON)
			if err != nil {
				return 0, err
			}
			matches := true
			for name, value := range tags {
				if bucketTags[name] != value {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
		}
		matched = append(matched, key)
	}
	for _, key := range matched {
		delete(s.hot, key)
		s.removeHotReplacementLocked(key)
	}
	return int64(len(matched)), nil
}
