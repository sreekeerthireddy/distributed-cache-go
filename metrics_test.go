package distcache

import "testing"

func TestShardStats(t *testing.T) {
	c := NewCache(4, 1000, 128, WithHotKeys(false))
	defer c.Close()

	c.Put("a", []byte("1"), 0) // 1 write
	c.Get("a")                 // 1 hit
	c.Get("missing")           // 1 miss

	var st ShardStat
	for _, s := range c.ShardStats() {
		st.Hits += s.Hits
		st.Misses += s.Misses
		st.Writes += s.Writes
	}
	if st.Writes < 1 || st.Hits < 1 || st.Misses < 1 {
		t.Fatalf("stats = %+v, want >=1 of each", st)
	}
}
