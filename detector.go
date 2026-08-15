package distcache

import (
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// spaceSaving is a bounded "heavy hitters" tracker: it monitors at most
// `capacity` keys and answers "which keys are hottest right now?" using the
// Space-Saving (Stream-Summary) algorithm — O(1) amortized per update, O(capacity)
// memory, and no false negatives for any key above 1/capacity of the stream.
//
// It is NOT safe for concurrent use — the enclosing detector serializes access.
type spaceSaving struct {
	capacity int
	counts   map[string]*ssCounter
}

// ssCounter is one monitored key's estimate. count is a float so decay can halve
// it cheaply (see decayOnce, later). error is the max we may have over-counted:
// the true count is guaranteed to lie in [count-error, count]. We promote on the
// conservative lower bound so a key that only looks hot because it inherited a big
// eviction credit doesn't get falsely flagged.
type ssCounter struct {
	count float64
	error float64
}

func newSpaceSaving(capacity int) *spaceSaving {
	return &spaceSaving{
		capacity: capacity,
		counts:   make(map[string]*ssCounter, capacity),
	}
}

// observe records one (already-sampled) access to key, adding weight (normally 1).
func (s *spaceSaving) observe(key string, weight float64) {
	if c, ok := s.counts[key]; ok { // already monitored → just bump it
		c.count += weight
		return
	}
	if len(s.counts) < s.capacity { // free slot → adopt the newcomer, no debt
		s.counts[key] = &ssCounter{count: weight}
		return
	}
	// Table full: evict the minimum-count key and hand its slot to the newcomer.
	// The newcomer might have been here before and been evicted, so the most it
	// could have silently accrued is cMin — we credit it that (count = cMin+weight)
	// and record cMin as error so we still know its true-count lower bound.
	minKey, minC := s.minSlot()
	cMin := minC.count
	delete(s.counts, minKey)
	s.counts[key] = &ssCounter{count: cMin + weight, error: cMin}
}

// minSlot returns the monitored key with the smallest count. O(capacity) scan —
// fine for capacity≈256 and only hit on eviction. (A Stream-Summary bucket list
// would make this O(1); not worth the complexity here.)
func (s *spaceSaving) minSlot() (string, *ssCounter) {
	var minKey string
	var minC *ssCounter
	for k, c := range s.counts {
		if minC == nil || c.count < minC.count {
			minKey, minC = k, c
		}
	}
	return minKey, minC
}

// hotSet is the published set of currently-hot keys. It is immutable once
// published; the background goroutine swaps in a new one (copy-on-write), exactly
// like the topology pointer — so the hot-path check is a single lock-free load.
type hotSet map[string]struct{}

// detector watches the access stream and maintains a lock-free set of hot keys.
// It samples the hot path cheaply, feeds a Space-Saving table under a small mutex
// (touched only on sampled ops), and a background goroutine periodically decays
// and republishes the hot set (decayOnce / loop / Close).
type detector struct {
	mu sync.Mutex // guards ss; taken only on the ~1/N sampled path
	ss *spaceSaving

	sample func() bool // true ~1/N of calls; goroutine-safe & lock-free. Injectable for tests.

	hot atomic.Pointer[hotSet] // published hot set; read lock-free on every op

	decayFactor float64 // multiply counts each tick (0.5 = halve)
	promote     float64 // become hot when conservative count ≥ promote (table-count units)
	demote      float64 // stay hot until conservative count < demote  (hysteresis: demote < promote)
	maxHot      int     // safety cap on hot-set size (backstop, not the gate)

	interval time.Duration
	stop     chan struct{}
}

// detectorConfig holds the detector's tunables. Thresholds are in table-count
// units — use rateToCount to derive them from an ops/sec target.
type detectorConfig struct {
	capacity    int           // Space-Saving table size (m)
	sampleRate  uint32        // 1/sampleRate sampling (power of two)
	interval    time.Duration // decay + publish tick
	decayFactor float64       // 0.5
	promote     float64
	demote      float64
	maxHot      int
}

func newDetector(cfg detectorConfig) *detector {
	d := &detector{
		ss:          newSpaceSaving(cfg.capacity),
		sample:      newSampler(cfg.sampleRate),
		decayFactor: cfg.decayFactor,
		promote:     cfg.promote,
		demote:      cfg.demote,
		maxHot:      cfg.maxHot,
		interval:    cfg.interval,
		stop:        make(chan struct{}),
	}
	empty := make(hotSet)
	d.hot.Store(&empty) // never nil after construction
	return d
}

// record is the hot-path entry point, called on every Get/Put. It samples FIRST
// (cheap, no shared state), and only takes the lock on a sampled hit — so the
// detector's mutex sees ~1/N of traffic, never the full celebrity flood.
func (d *detector) record(key string) {
	if !d.sample() {
		return
	}
	d.mu.Lock()
	d.ss.observe(key, 1)
	d.mu.Unlock()
}

// isHot reports whether key is in the currently-published hot set — one lock-free
// atomic load, safe on the hot path. nil-safe before the first publish.
func (d *detector) isHot(key string) bool {
	hs := d.hot.Load()
	if hs == nil {
		return false
	}
	_, ok := (*hs)[key]
	return ok
}

// decayOnce decays every count, recomputes the hot set with hysteresis, caps it,
// and publishes it. Called by loop() on a ticker AND directly by tests (so hotness
// is fully deterministic without touching wall-clock or the goroutine).
func (d *detector) decayOnce() {
	type cand struct {
		key string
		est float64
	}

	d.mu.Lock()
	old := d.hot.Load() // current hot set, for hysteresis
	cands := make([]cand, 0, len(d.ss.counts))
	for key, c := range d.ss.counts {
		c.count *= d.decayFactor // decay: recent traffic weighs more
		c.error *= d.decayFactor

		est := c.count - c.error // conservative lower-bound estimate
		bar := d.promote
		if old != nil {
			if _, wasHot := (*old)[key]; wasHot {
				bar = d.demote // already hot → lower bar to STAY hot (hysteresis)
			}
		}
		if est >= bar {
			cands = append(cands, cand{key, est})
		}
	}
	d.mu.Unlock() // everything below uses only the local slice — no lock needed

	// Safety cap: if more than maxHot qualify, keep the hottest (backstop only —
	// the absolute promote threshold already bounds this in practice).
	if len(cands) > d.maxHot {
		sort.Slice(cands, func(i, j int) bool { return cands[i].est > cands[j].est })
		cands = cands[:d.maxHot]
	}

	next := make(hotSet, len(cands))
	for _, c := range cands {
		next[c.key] = struct{}{}
	}
	d.hot.Store(&next) // atomic copy-on-write publish
}

// start launches the background decay/publish loop. Construction does not start it,
// so tests can drive decayOnce directly with no background activity.
func (d *detector) start() { go d.loop() }

func (d *detector) loop() {
	t := time.NewTicker(d.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			d.decayOnce()
		case <-d.stop:
			return
		}
	}
}

// Close stops the background loop. Call exactly once.
func (d *detector) Close() { close(d.stop) }

// newSampler returns a goroutine-safe function that returns true ~1/rate of the
// time. rate must be a power of two so we can test the low bits with a mask — the
// cheapest possible decision. math/rand/v2's top-level generator is per-goroutine
// and lock-free, so this touches no shared state (no shared sample counter).
func newSampler(rate uint32) func() bool {
	mask := rate - 1 // rate=16 → mask=0b1111 → true when low 4 bits are 0 → 1/16
	return func() bool {
		return rand.Uint32()&mask == 0
	}
}

// rateToCount converts a per-key ops/sec threshold into the decayed, sampled count
// the detector compares against. With 1/sampleRate sampling and exponential decay
// of factor f every interval, the effective averaging window is interval/(-ln f)
// (≈1.44·interval for f=0.5), and a key at true rate r settles at a steady-state
// count of ~ (r/sampleRate) × window. This is what ties the threshold to a MEASURED
// single-shard throughput instead of a magic number.
func rateToCount(ratePerSec float64, interval time.Duration, decayFactor float64, sampleRate uint32) float64 {
	window := interval.Seconds() / -math.Log(decayFactor)
	return ratePerSec / float64(sampleRate) * window
}
