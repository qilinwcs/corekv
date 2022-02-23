// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "math"

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	//在这里实现判断一个数据是否在bloom过滤器中
	//思路大概是经过K个Hash函数计算，判读对应位置是否被标记为1
	if len(f) < 2 {
		return false
	}
	mBits := 8 * (len(f) - 1)
	k := f[len(f)-1]
	if k > 30 {
		return true
	}
	delta := h>>17 | h<<15 //将key HASH 前15字节和后17字节交换
	for i := uint8(0); i < k; i++ {
		bitPos := h % uint32(mBits) //根据布隆过滤器位数取余，计算K次 分布在过滤器上的位置
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	//传入参数numEntries是bloom中存储的数据个数，fp是false positive假阳性率
	//假设传入数据元素个数为 numEntries，设定误差为 fp，则返回 布隆过滤器 位数组大小 m
	//阅读bloom论文实现，m = -1*numEntries（lne p）/((lne 2)^2)，得到浮点数需向上取整
	//hash函数个数K = m/n * ln2(0.69314718056)
	//此函数返回m/n的值
	m := -1 * float64(numEntries) * math.Log(fp) / math.Pow(0.69314718056, 2)
	locs := math.Ceil(m / float64(numEntries))
	return int(locs)
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	//布隆过滤器属于一次创建，所以从keys中可以获取到n值，根据 locs（m/n）可以计算出 需要布隆过滤器的位数组长度
	//在这里实现将一个Key的HASH值放入到bloom过滤器中
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	num := len(keys)
	mBits := bitsPerKey * num
	if mBits < 64 {
		mBits = 64
	}
	mBytes := (mBits + 7) / 8
	mBits = mBytes * 8
	filter := make([]byte, mBytes+1) //布隆过滤器返回值
	for _, n := range keys {
		delta := n>>17 | n<<15 //将key HASH 前15字节和后17字节交换
		for i := uint32(0); i < k; i++ {
			bitPos := n % uint32(mBits) //根据布隆过滤器位数取余，计算K次 分布在过滤器上的位置
			filter[bitPos/8] |= 1 << (bitPos % 8)
			n += delta
		}
	}
	filter[mBytes] = uint8(k)
	return filter
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	//根据原理，这里应该根据 m/n * ln2 计算出 hash次数K，进行K轮hash
	//在这里实现高效的HashFunction
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
