package distcache

import (
	"fmt"
	"testing"
)

func TestRingEmpty(t *testing.T) {
	r := NewRing(128)
	if _, ok := r.GetNode("anything"); ok {
		t.Fatal("empty ring should return ok=false")
	}
}

func TestRingSingleNode(t *testing.T) {
	r := NewRing(128)
	r.AddNode("A")
	for i := 0; i < 100; i++ {
		node, ok := r.GetNode(fmt.Sprintf("key-%d", i))
		if !ok || node != "A" {
			t.Fatalf("single node: key-%d -> %q ok=%v, want A", i, node, ok)
		}
	}
}

func TestRingDeterministic(t *testing.T) {
	r := NewRing(128)
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")
	n1, _ := r.GetNode("hello")
	n2, _ := r.GetNode("hello")
	if n1 != n2 {
		t.Fatalf("same key mapped to %q then %q — must be deterministic", n1, n2)
	}
}

func TestRingDistribution(t *testing.T) {
	r := NewRing(128)
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")

	counts := map[string]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		node, _ := r.GetNode(fmt.Sprintf("key-%d", i))
		counts[node]++
	}
	// Each of 3 nodes should get roughly n/3. Allow a generous ±30% band.
	expected := n / 3
	for _, node := range []string{"A", "B", "C"} {
		got := counts[node]
		if got < expected*7/10 || got > expected*13/10 {
			t.Errorf("node %s got %d keys, want ~%d (±30%%)", node, got, expected)
		}
	}
}

func TestRingMinimalReshuffle(t *testing.T) {
	r := NewRing(128)
	r.AddNode("A")
	r.AddNode("B")
	r.AddNode("C")

	const n = 30000
	before := make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, _ := r.GetNode(key)
		before[key] = node
	}

	r.AddNode("D") // add a 4th node

	moved := 0
	for key, oldNode := range before {
		newNode, _ := r.GetNode(key)
		if newNode != oldNode {
			moved++
			// Consistent-hashing property: adding D should only move keys TO D,
			// never shuffle keys between existing nodes.
			if newNode != "D" {
				t.Fatalf("key %s moved %s -> %s, but only moves to D are allowed", key, oldNode, newNode)
			}
		}
	}

	// Adding the 4th node should move ~1/4 of keys. Allow a band.
	frac := float64(moved) / float64(n)
	if frac < 0.15 || frac > 0.35 {
		t.Errorf("adding D moved %.1f%% of keys, want ~25%%", frac*100)
	}
}

func TestRingAddRemove(t *testing.T) {
	r := NewRing(128)
	r.AddNode("A")
	r.AddNode("A") // idempotent — no-op
	r.AddNode("B")

	// remove B -> all keys should now route to A
	r.RemoveNode("B")
	for i := 0; i < 100; i++ {
		node, ok := r.GetNode(fmt.Sprintf("key-%d", i))
		if !ok || node != "A" {
			t.Fatalf("after removing B, key-%d -> %q ok=%v, want A", i, node, ok)
		}
	}

	r.RemoveNode("nonexistent") // no-op, must not panic
	r.RemoveNode("A")
	if _, ok := r.GetNode("key-0"); ok {
		t.Fatal("after removing all nodes, ring should be empty (ok=false)")
	}
}
