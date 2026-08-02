package distcache

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// Ring maps keys to nodes using consistent hashing with virtual nodes.
// Each physical node is placed at `vnodes` positions on the ring so that
// keys — and rebalancing — distribute evenly. A "node" here == a shard.
// The Ring is pure routing: it holds NO cached data.
type Ring struct {
	vnodes    int                 // virtual nodes per physical node
	positions []uint32            // SORTED ring positions (all vnodes of all nodes)
	nodeAt    map[uint32]string   // ring position -> node ID
	nodes     map[string]struct{} // set of physical node IDs (membership)
}

// NewRing creates an empty ring. vnodes controls distribution smoothness
// (more vnodes = more even); ~128 is typical.
func NewRing(vnodes int) *Ring {
	return &Ring{
		vnodes: vnodes,
		nodeAt: make(map[uint32]string),
		nodes:  make(map[string]struct{}),
	}
}

// hashKey maps a string to a position on the 32-bit ring. It runs FNV-1a
// then a murmur3 finalizer (mix32) — the finalizer improves avalanche so
// structured inputs like "A#0", "A#1", ... spread EVENLY around the ring
// instead of clustering (which badly skews per-node load).
func hashKey(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return mix32(h.Sum32())
}

// mix32 is the murmur3 fmix32 finalizer — a strong bit-mixing step that
// decorrelates similar inputs.
func mix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// AddNode places a node on the ring at `vnodes` scattered positions.
// No-op if the node is already present.
func (r *Ring) AddNode(nodeID string) {
	if _, exists := r.nodes[nodeID]; exists {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		pos := hashKey(nodeID + "#" + strconv.Itoa(i)) // "A#0", "A#1", ...
		r.positions = append(r.positions, pos)
		r.nodeAt[pos] = nodeID
	}
	sort.Slice(r.positions, func(a, b int) bool { return r.positions[a] < r.positions[b] })
}

// RemoveNode removes a node and all its virtual positions from the ring.
// No-op if the node isn't present. Removes routing entries only — no data.
func (r *Ring) RemoveNode(nodeID string) {
	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	delete(r.nodes, nodeID)
	kept := r.positions[:0] // in-place filter (reuse backing array; stays sorted)
	for _, pos := range r.positions {
		if r.nodeAt[pos] == nodeID {
			delete(r.nodeAt, pos)
			continue
		}
		kept = append(kept, pos)
	}
	r.positions = kept
}

// GetNode returns the node that owns key (first position clockwise from
// hash(key), wrapping around). Returns false if the ring is empty.
func (r *Ring) GetNode(key string) (string, bool) {
	if len(r.positions) == 0 {
		return "", false
	}
	h := hashKey(key)
	idx := sort.Search(len(r.positions), func(i int) bool { // first position >= h
		return r.positions[i] >= h
	})
	if idx == len(r.positions) {
		idx = 0 // wrap around the ring
	}
	return r.nodeAt[r.positions[idx]], true
}
