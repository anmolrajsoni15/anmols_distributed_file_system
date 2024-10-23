package dht

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

const (
	k             = 20    // k-bucket size
	alpha         = 3     // alpha is the degree of parallelism in network calls
	bucketSize    = 160   // Kademlia DHT uses 160-bit keys
	pingInterval  = 15 * time.Minute
)

type NodeID [20]byte

func NewNodeID(data string) NodeID {
	sha := sha1.Sum([]byte(data))
	return NodeID(sha)
}

type Node struct {
	ID      NodeID
	Address string
	LastSeen time.Time
}

type Bucket struct {
	nodes []Node
	mu    sync.RWMutex
}

type DHT struct {
	nodeID  NodeID
	buckets [bucketSize]*Bucket
	mu      sync.RWMutex
}

func NewDHT(nodeID NodeID) *DHT {
	dht := &DHT{nodeID: nodeID}
	for i := 0; i < bucketSize; i++ {
		dht.buckets[i] = &Bucket{}
	}
	return dht
}

func (dht *DHT) AddNode(node Node) {
	bucketIndex := dht.getBucketIndex(node.ID)
	bucket := dht.buckets[bucketIndex]

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for i, n := range bucket.nodes {
		if n.ID == node.ID {
			// Move to end of bucket (most recently seen)
			bucket.nodes = append(bucket.nodes[:i], bucket.nodes[i+1:]...)
			bucket.nodes = append(bucket.nodes, node)
			return
		}
	}

	if len(bucket.nodes) < k {
		bucket.nodes = append(bucket.nodes, node)
	} else {
		// If bucket is full, ping the least-recently seen node
		// If it responds, move it to the end, otherwise replace it
		// This is simplified for now - we'll just replace
		bucket.nodes = append(bucket.nodes[1:], node)
	}
}

func (dht *DHT) FindClosestNodes(targetID NodeID) []Node {
	var closest []Node
	seen := make(map[string]bool)

	for _, bucket := range dht.buckets {
		bucket.mu.RLock()
		for _, node := range bucket.nodes {
			if !seen[node.Address] {
				closest = append(closest, node)
				seen[node.Address] = true
			}
		}
		bucket.mu.RUnlock()
	}

	// Sort by XOR distance
	sort.Slice(closest, func(i, j int) bool {
		distI := xor(closest[i].ID, targetID)
		distJ := xor(closest[j].ID, targetID)
		return lessBytes(distI[:], distJ[:])
	})

	if len(closest) > k {
		closest = closest[:k]
	}

	return closest
}

func (dht *DHT) getBucketIndex(id NodeID) int {
	// Find the first different bit
	for i := 0; i < len(id); i++ {
		for j := 0; j < 8; j++ {
			if ((dht.nodeID[i] >> uint8(7-j)) & 0x1) != ((id[i] >> uint8(7-j)) & 0x1) {
				return i*8 + j
			}
		}
	}
	return bucketSize - 1
}

func xor(a, b NodeID) NodeID {
	var res NodeID
	for i := 0; i < len(a); i++ {
		res[i] = a[i] ^ b[i]
	}
	return res
}

func lessBytes(a, b []byte) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// String returns a hex representation of the NodeID
func (id NodeID) String() string {
	return hex.EncodeToString(id[:])
}