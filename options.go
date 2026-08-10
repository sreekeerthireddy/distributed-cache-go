package distcache

import "time"

// Option configures a Cache. Defaults enable hot-key detection with sane values.
type Option func(*cacheOptions)

type cacheOptions struct {
	hotKeys       bool          // enable hotness detection
	ssCapacity    int           // Space-Saving table size (m)
	sampleRate    uint32        // 1/sampleRate sampling (power of two)
	decayInterval time.Duration // decay + publish tick
	decayFactor   float64       // 0.5 = halve each tick
	shardMaxRate  float64       // MEASURED single-shard throughput S_max (ops/sec)
	promoteFrac   float64       // α: promote a key above promoteFrac·S_max
	demoteFrac    float64       // hysteresis lower bar (< promoteFrac)
	maxHot        int           // hot-set safety cap
	replicas      int           // R for read replication
	replicaTTL    time.Duration // TTL on hot-key replicas (bounds staleness after demotion)
	counterShards int           // sub-counters per sharded counter (Incr/GetCounter)
}

func defaultOptions() cacheOptions {
	return cacheOptions{
		hotKeys:       true,
		ssCapacity:    256,
		sampleRate:    16,
		decayInterval: time.Second,
		decayFactor:   0.5,
		shardMaxRate:  2_600_000, // measured 1-shard throughput from our benchmark
		promoteFrac:   0.15,      // ~390K ops/sec on one key → shard it
		demoteFrac:    0.075,     // ~195K ops/sec → stay sharded until below this
		maxHot:        16,
		replicas:      4,
		replicaTTL:    10 * time.Second,
		counterShards: 8,
	}
}

// WithHotKeys toggles the whole hot-key detection feature.
func WithHotKeys(enabled bool) Option { return func(o *cacheOptions) { o.hotKeys = enabled } }

// WithHotKeyThreshold sets the physical anchor: the measured single-shard throughput
// and the promote/demote fractions of it (α). Ties the threshold to a real number.
func WithHotKeyThreshold(shardMaxRate, promoteFrac, demoteFrac float64) Option {
	return func(o *cacheOptions) {
		o.shardMaxRate, o.promoteFrac, o.demoteFrac = shardMaxRate, promoteFrac, demoteFrac
	}
}

// WithReplicas sets R, the replication factor for hot read-keys.
func WithReplicas(r int) Option { return func(o *cacheOptions) { o.replicas = r } }

// WithReplicaTTL sets how long hot-key replicas live before needing a refresh.
func WithReplicaTTL(d time.Duration) Option { return func(o *cacheOptions) { o.replicaTTL = d } }

// WithDecayInterval sets how often the detector decays counts and republishes the
// hot set. The promote/demote thresholds recalibrate to this interval, so shorter
// intervals just react faster (useful for short-lived processes and benchmarks).
func WithDecayInterval(d time.Duration) Option { return func(o *cacheOptions) { o.decayInterval = d } }

// WithCounterShards sets how many sub-counters a sharded counter is split into.
func WithCounterShards(n int) Option { return func(o *cacheOptions) { o.counterShards = n } }
