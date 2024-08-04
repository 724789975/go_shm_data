package lru_cache

import (
	"cmp"
)

// 定义LRU缓存结构
type LRUCache[K cmp.Ordered, V any] struct {
	size     int                  // 实际占用大小
	capacity int                  // 最大存储容量
	cache    map[K]*DLinkedNode[K, V] // 缓存key与value的映射关系
	// 按照缓存key的访问时间，从近到远构造双向链表，
	// 头结点为最近访问的key，尾节点为很久不访问的节点。
	head, tail *DLinkedNode[K, V]
	eliminate func(K, V) // 缓存数据移除时调用的回调函数
}
 
// 数据节点的结构
type DLinkedNode[K any, V any] struct {
	key    K         // 数据的key
	value  V         // 数据
	prev, next *DLinkedNode[K, V] // 数据的前后节点指针，用于构建双向链表
}
 
// LRU缓存构造器
func CreateLRUCache[K cmp.Ordered, V any](capacity int, eliminate func(K, V)) *LRUCache[K, V] {
	if capacity < 2 {
		panic("capacity should be greater than 2")
	}
	l := &LRUCache[K, V]{
		cache:    map[K]*DLinkedNode[K, V]{},
		head:     nil,
		tail:     nil,
		capacity: capacity,
		size:     0,
	}
	l.eliminate = eliminate
	return l
}

func (l *LRUCache[K, V]) Size() int {
	return l.size
}

// 通过key获取缓存数据，然后把这个数据挪到链表头，表示最近访问的节点
func (l *LRUCache[K, V]) Get(key K) (V, bool) {
	var v V
	node, ok := l.cache[key];
	if !ok {
		return v, ok
	}
	l.moveToHead(node)
	return node.value, ok
}

// 移动数据节点到链表头
func (l *LRUCache[K, V]) moveToHead(node *DLinkedNode[K, V]) {
	if node == l.head {
		return
	}
	l.removeNode(node)
	l.addToHead(node)
}

// 将数据节点添加到链表头
func (l *LRUCache[K, V]) addToHead(node *DLinkedNode[K, V]) {
	node.next = l.head
	node.prev = nil
	if l.head != nil{
		l.head.prev = node
	}
	l.head = node
	if l.tail == nil {
		l.tail = node
	}
}

// 从链表删除节点
func (l *LRUCache[K, V]) removeNode(node *DLinkedNode[K, V]) {
	if node.prev == nil {
		l.head = node.next
	} else {
		node.prev.next = node.next
	}

	if node.next == nil {
		l.tail = node.prev
	}else{
		node.next.prev = node.prev
	}
}

// 移除链表尾部的节点
func (l *LRUCache[K, V]) removeTail() *DLinkedNode[K, V] {
	node := l.tail
	l.removeNode(node)
	return node
}

// 通过key修改缓存数据，有此key则更新value，无此key则添加新key移除老key
func (l *LRUCache[K, V]) Put(key K, value V) {
	node, ok := l.cache[key]
	if !ok {
		node := &DLinkedNode[K, V]{ key: key, value: value}
		l.cache[key] = node
		l.addToHead(node)
		l.size++
		if l.size > l.capacity {
			removed := l.removeTail()
			delete(l.cache, removed.key)
			l.eliminate(removed.key, removed.value)
			l.size--
		}
	} else {
		node.value = value
		l.moveToHead(node)
	}
}
 
 

 
 
