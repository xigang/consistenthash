package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const defaultReplicas = 20

//Hash specified hash algorithm
type Hash func(data []byte) uint32

type Map struct {
	hash     Hash           //hash algorithm
	replicas int            //replicas
	keys     []int          //Sorted
	hashMap  map[int]string //The mapping between the virtual node and the real node
	mu       sync.RWMutex
}

// New creates a new Consistent object with a default setting of 20 replicas for each entry.
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}

	if replicas == 0 {
		m.replicas = defaultReplicas
	}
	return m
}

//Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

//Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.mu.Lock()
			m.hashMap[hash] = key
			m.mu.Unlock()
		}
	}
	sort.Ints(m.keys)
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// 顺时针“行走”，找到第一个大于哈希值的节点
	for _, v := range m.keys {
		if v >= hash {
			return m.hashMap[v] // 返回真实节点
		}
	}

	// hash值大于最大节点哈希值的情况
	return m.hashMap[m.keys[0]]
}

// Remove removes an element from the hash.
func (m *Map) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := int(m.hash([]byte(key)))
	if _, ok := m.hashMap[hash]; !ok {
		return
	}

	//delete hash map from hashMap
	delete(m.hashMap, hash)

	//delete hash value from keys.
	for k, v := range m.keys {
		if v == hash {
			m.keys = append(m.keys[:k], m.keys[k+1:]...)
		}
	}
	sort.Ints(m.keys)
}
