package lru_cache_test

import (
	"goshm/lru_cache"
	"testing"
)

func Test2(t *testing.T) {
	c := lru_cache.CreateLRUCache(2, func(i int64, s string) {
		t.Logf("evicted %d %s", i, s)
	})

	c.Put(1, "a")
	c.Put(2, "b")
	c.Put(3, "c")
	println(c.Get(1))
	c.Put(4, "d")
	println(c.Get(4))
	println(c.Get(3))
	c.Put(5, "e")
	println(c.Get(4))
	println(c.Get(3))
}
