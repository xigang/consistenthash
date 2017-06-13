package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

//Hash specified hash algorithm
type Hash func(data []byte) uint32

type Map struct {
	hash     Hash           //选择hash算法
	replicas int            //节点的副本数
	keys     []int          //需要排序
	hashMap  map[int]string //真实节点对应的虚拟节点
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
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
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

//Gets the closest item in the hash to the provided key
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// 顺时针“行走”，找到第一个大于哈希值的节点
	for _, v := range m.keys {
		if v >= hash {
			return m.hashMap[v]
		}
	}

	// hash值大于最大节点哈希值的情况
	return m.hashMap[m.keys[0]]
}
