/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bitmapdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/c2h5oh/datasize"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

var once sync.Once

var myType = 0

func initialize() {
	fmt.Println(fmt.Sprintf("zjg, myType:%v", myType))
}

const MaxUint32 = 1<<32 - 1

type ToBitamp interface {
	ToBitmap() (*roaring64.Bitmap, error)
}

var roaringPool = sync.Pool{
	New: func() any {
		return roaring.New()
	},
}

func NewBitmap() *roaring.Bitmap {
	a := roaringPool.Get().(*roaring.Bitmap)
	a.Clear()
	return a
}
func ReturnToPool(a *roaring.Bitmap) {
	if a == nil {
		return
	}
	roaringPool.Put(a)
}

var roaring64Pool = sync.Pool{
	New: func() any {
		return roaring64.New()
	},
}

func NewBitmap64() *roaring64.Bitmap {
	a := roaring64Pool.Get().(*roaring64.Bitmap)
	a.Clear()
	return a
}
func ReturnToPool64(a *roaring64.Bitmap) {
	if a == nil {
		return
	}
	roaring64Pool.Put(a)
}

const ChunkLimit = uint64(1950 * datasize.B) // threshold beyond which MDBX overflow pages appear: 4096 / 2 - (keySize + 8)

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeft(bm *roaring.Bitmap, sizeLimit uint64) *roaring.Bitmap {
	if myType == 0 {
		return CutLeftRaw(bm, sizeLimit)
	} else if myType == 1 {
		return CutLeftGPT(bm, sizeLimit)
	} else if myType == 2 {
		return CutLeftDoubao(bm, sizeLimit)
	}

	return nil
}

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeftRaw(bm *roaring.Bitmap, sizeLimit uint64) *roaring.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		lft := roaring.New()
		lft.AddRange(uint64(bm.Minimum()), uint64(bm.Maximum())+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	from := uint64(bm.Minimum())
	minMax := bm.Maximum() - bm.Minimum()
	to := sort.Search(int(minMax), func(i int) bool { // can be optimized to avoid "too small steps", but let's leave it for readability
		lft := roaring.New() // bitmap.Clear() method intentionally not used here, because then serialized size of bitmap getting bigger
		lft.AddRange(from, from+uint64(i)+1)
		lft.And(bm)
		lft.RunOptimize()
		return lft.GetSerializedSizeInBytes() > sizeLimit
	})

	lft := roaring.New()
	lft.AddRange(from, from+uint64(to)) // no +1 because sort.Search returns element which is just higher threshold - but we need lower
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()
	return lft
}

func CutLeftGPT(bm *roaring.Bitmap, sizeLimit uint64) *roaring.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		// 如果整个 bitmap 小于等于 sizeLimit，则直接返回一个包含所有值的新 bitmap
		lft := roaring.New()
		lft.AddRange(uint64(bm.Minimum()), uint64(bm.Maximum())+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	from := uint64(bm.Minimum())
	minMax := bm.Maximum() - bm.Minimum()

	// 创建一个用于临时计算的 bitmap 实例
	tempBitmap := roaring.New()
	defer tempBitmap.Release() // 确保释放资源

	// 使用 sort.Search 查找合适的范围
	to := sort.Search(int(minMax), func(i int) bool {
		tempBitmap.Clear()
		tempBitmap.AddRange(from, from+uint64(i)+1)
		tempBitmap.And(bm)
		tempBitmap.RunOptimize()
		return tempBitmap.GetSerializedSizeInBytes() > sizeLimit
	})

	// 创建最终结果 bitmap
	lft := roaring.New()
	lft.AddRange(from, from+uint64(to))
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()

	return lft
}

func binarySearchRaw(from uint64, minMax uint64, bm *roaring.Bitmap, sizeLimit uint64) int {
	low, high := 0, int(minMax)
	for low < high {
		mid := (low + high) / 2
		temp := roaring.New()
		temp.AddRange(from, from+uint64(mid))
		temp.And(bm)
		temp.RunOptimize()
		if temp.GetSerializedSizeInBytes() > sizeLimit {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

// CutLeft - cut from bitmap `targetSize` bytes from left
// removing lft part from `bm`
// returns nil on zero cardinality
func CutLeftDoubao(bm *roaring.Bitmap, sizeLimit uint64) *roaring.Bitmap {
	var result *roaring.Bitmap
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		if result == nil {
			result = bm.Clone()
		} else {
			result.Or(bm.Clone())
		}
		result.RunOptimize()
		return result
	}

	from := uint64(bm.Minimum())
	minMax := uint64(bm.Maximum() - bm.Minimum())

	to := binarySearchRaw(from, minMax, bm, sizeLimit)
	result = roaring.New()
	result.AddRange(from, from+uint64(to))
	result.And(bm)
	result.RunOptimize()
	return result
}

func WalkChunks(bm *roaring.Bitmap, sizeLimit uint64, f func(chunk *roaring.Bitmap, isLast bool) error) error {
	for bm.GetCardinality() > 0 {
		if err := f(CutLeft(bm, sizeLimit), bm.GetCardinality() == 0); err != nil {
			return err
		}
	}
	return nil
}

func WalkChunkWithKeys(k []byte, m *roaring.Bitmap, sizeLimit uint64, f func(chunkKey []byte, chunk *roaring.Bitmap) error) error {
	return WalkChunks(m, sizeLimit, func(chunk *roaring.Bitmap, isLast bool) error {
		chunkKey := make([]byte, len(k)+4)
		copy(chunkKey, k)
		if isLast {
			binary.BigEndian.PutUint32(chunkKey[len(k):], ^uint32(0))
		} else {
			binary.BigEndian.PutUint32(chunkKey[len(k):], chunk.Maximum())
		}
		return f(chunkKey, chunk)
	})
}

// TruncateRange - gets existing bitmap in db and call RemoveRange operator on it.
// starts from hot shard, stops when shard not overlap with [from-to)
// !Important: [from, to)
func TruncateRange(db kv.RwTx, bucket string, key []byte, to uint32) error {
	chunkKey := make([]byte, len(key)+4)
	copy(chunkKey, key)
	binary.BigEndian.PutUint32(chunkKey[len(chunkKey)-4:], to)
	bm, err := Get(db, bucket, key, to, MaxUint32)
	if err != nil {
		return err
	}

	if bm.GetCardinality() > 0 && to <= bm.Maximum() {
		bm.RemoveRange(uint64(to), uint64(bm.Maximum())+1)
	}

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := Walk(c, chunkKey, 0, func(k, v []byte) (bool, error) {
		if !bytes.HasPrefix(k, key) {
			return false, nil
		}
		if err := db.Delete(bucket, k); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)
	return WalkChunkWithKeys(key, bm, ChunkLimit, func(chunkKey []byte, chunk *roaring.Bitmap) error {
		buf.Reset()
		if _, err := chunk.WriteTo(buf); err != nil {
			return err
		}
		return db.Put(bucket, chunkKey, libcommon.Copy(buf.Bytes()))
	})
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get(db kv.Tx, bucket string, key []byte, from, to uint32) (*roaring.Bitmap, error) {
	var chunks []*roaring.Bitmap

	fromKey := make([]byte, len(key)+4)
	copy(fromKey, key)
	binary.BigEndian.PutUint32(fromKey[len(fromKey)-4:], from)
	c, err := db.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for k, v, err := c.Seek(fromKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := NewBitmap()
		defer ReturnToPool(bm)
		if _, err := bm.ReadFrom(bytes.NewReader(v)); err != nil {
			return nil, err
		}
		chunks = append(chunks, bm)
		if binary.BigEndian.Uint32(k[len(k)-4:]) >= to {
			break
		}
	}
	if len(chunks) == 0 {
		return roaring.New(), nil
	}
	return roaring.FastOr(chunks...), nil
}

// SeekInBitmap - returns value in bitmap which is >= n
//
//nolint:deadcode
func SeekInBitmap(m *roaring.Bitmap, n uint32) (found uint32, ok bool) {
	i := m.Iterator()
	i.AdvanceIfNeeded(n)
	ok = i.HasNext()
	if ok {
		found = i.Next()
	}
	return found, ok
}

func CutLeft64(bm *roaring64.Bitmap, sizeLimit uint64) *roaring64.Bitmap {
	once.Do(initialize)
	if myType == 0 {
		return CutLeft64Raw(bm, sizeLimit)
	} else if myType == 1 {
		return CutLeft64GPT(bm, sizeLimit)
	} else if myType == 2 {
		return CutLeft64Doubao(bm, sizeLimit)
	}

	return nil
}

func CutLeft64Raw(bm *roaring64.Bitmap, sizeLimit uint64) *roaring64.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		lft := roaring64.New()
		lft.AddRange(bm.Minimum(), bm.Maximum()+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	from := bm.Minimum()
	minMax := bm.Maximum() - bm.Minimum()
	count := 0

	to := sort.Search(int(minMax), func(i int) bool { // can be optimized to avoid "too small steps", but let's leave it for readability
		lft := roaring64.New() // bitmap.Clear() method intentionally not used here, because then serialized size of bitmap getting bigger
		lft.AddRange(from, from+uint64(i)+1)
		lft.And(bm)
		lft.RunOptimize()
		count += 1
		return lft.GetSerializedSizeInBytes() > sizeLimit
	})

	lft := roaring64.New()
	lft.AddRange(from, from+uint64(to)) // no +1 because sort.Search returns element which is just higher threshold - but we need lower
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()
	return lft
}

func CutLeft64GPT(bm *roaring64.Bitmap, sizeLimit uint64) *roaring64.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		// 处理 map size 小于等于 sizeLimit 的情况
		lft := roaring64.New()
		lft.AddRange(bm.Minimum(), bm.Maximum()+1)
		lft.And(bm)
		lft.RunOptimize()
		bm.Clear()
		return lft
	}

	// 初始化变量
	from := bm.Minimum()
	minMax := bm.Maximum() - bm.Minimum()

	// 创建一个用于测试大小的临时 Bitmap
	tempBitmap := roaring64.New()

	// 使用 sort.Search 查找合适的范围
	to := sort.Search(int(minMax), func(i int) bool {
		tempBitmap.Clear() // 清除之前的内容
		tempBitmap.AddRange(from, from+uint64(i)+1)
		tempBitmap.And(bm)
		tempBitmap.RunOptimize()
		return tempBitmap.GetSerializedSizeInBytes() > sizeLimit
	})

	// 创建最终结果 Bitmap
	lft := roaring64.New()
	lft.AddRange(from, from+uint64(to)) // 不加 +1，因为 sort.Search 返回的是下界
	lft.And(bm)
	bm.RemoveRange(from, from+uint64(to))
	lft.RunOptimize()

	return lft
}

// 优化后的二分搜索函数
func binarySearch64(from, minMax uint64, bm *roaring64.Bitmap, sizeLimit uint64) int {
	low, high := 0, int(minMax)
	for low < high {
		mid := (low + high) / 2
		temp := roaring64.New()
		temp.AddRange(from, from+uint64(mid))
		temp.And(bm)
		temp.RunOptimize()
		if temp.GetSerializedSizeInBytes() > sizeLimit {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low
}

func CutLeft64Doubao(bm *roaring64.Bitmap, sizeLimit uint64) *roaring64.Bitmap {
	if bm.GetCardinality() == 0 {
		return nil
	}

	sz := bm.GetSerializedSizeInBytes()
	if sz <= sizeLimit {
		result := bm.Clone()
		result.RunOptimize()
		return result
	}

	from := bm.Minimum()
	minMax := bm.Maximum() - bm.Minimum()

	to := binarySearch64(from, minMax, bm, sizeLimit)
	result := roaring64.New()
	result.AddRange(from, from+uint64(to))
	result.And(bm)
	result.RunOptimize()
	return result
}

func WalkChunks64(bm *roaring64.Bitmap, sizeLimit uint64, f func(chunk *roaring64.Bitmap, isLast bool) error) error {
	for bm.GetCardinality() > 0 {
		if err := f(CutLeft64(bm, sizeLimit), bm.GetCardinality() == 0); err != nil {
			return err
		}
	}
	return nil
}

func WalkChunkWithKeys64(k []byte, m *roaring64.Bitmap, sizeLimit uint64, f func(chunkKey []byte, chunk *roaring64.Bitmap) error) error {
	return WalkChunks64(m, sizeLimit, func(chunk *roaring64.Bitmap, isLast bool) error {
		chunkKey := make([]byte, len(k)+8)
		copy(chunkKey, k)
		if isLast {
			binary.BigEndian.PutUint64(chunkKey[len(k):], ^uint64(0))
		} else {
			binary.BigEndian.PutUint64(chunkKey[len(k):], chunk.Maximum())
		}
		return f(chunkKey, chunk)
	})
}

// TruncateRange - gets existing bitmap in db and call RemoveRange operator on it.
// starts from hot shard, stops when shard not overlap with [from-to)
// !Important: [from, to)
func TruncateRange64(db kv.RwTx, bucket string, key []byte, to uint64) error {
	chunkKey := make([]byte, len(key)+8)
	copy(chunkKey, key)
	binary.BigEndian.PutUint64(chunkKey[len(chunkKey)-8:], to)
	bm, err := Get64(db, bucket, key, to, math.MaxUint64)
	if err != nil {
		return err
	}

	if bm.GetCardinality() > 0 && to <= bm.Maximum() {
		bm.RemoveRange(to, bm.Maximum()+1)
	}

	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	cDel, err := db.RwCursor(bucket)
	if err != nil {
		return err
	}
	defer cDel.Close()
	if err := Walk(c, chunkKey, 0, func(k, v []byte) (bool, error) {
		if !bytes.HasPrefix(k, key) {
			return false, nil
		}
		if err := cDel.Delete(k); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)
	return WalkChunkWithKeys64(key, bm, ChunkLimit, func(chunkKey []byte, chunk *roaring64.Bitmap) error {
		buf.Reset()
		if _, err := chunk.WriteTo(buf); err != nil {
			return err
		}
		return db.Put(bucket, chunkKey, libcommon.Copy(buf.Bytes()))
	})
}

// Get - reading as much chunks as needed to satisfy [from, to] condition
// join all chunks to 1 bitmap by Or operator
func Get64(db kv.Tx, bucket string, key []byte, from, to uint64) (*roaring64.Bitmap, error) {
	var chunks []*roaring64.Bitmap

	fromKey := make([]byte, len(key)+8)
	copy(fromKey, key)
	binary.BigEndian.PutUint64(fromKey[len(fromKey)-8:], from)

	c, err := db.Cursor(bucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for k, v, err := c.Seek(fromKey); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, key) {
			break
		}
		bm := NewBitmap64()
		defer ReturnToPool64(bm)
		_, err := bm.ReadFrom(bytes.NewReader(v))
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, bm)
		if binary.BigEndian.Uint64(k[len(k)-8:]) >= to {
			break
		}
	}

	if len(chunks) == 0 {
		return roaring64.New(), nil
	}
	return roaring64.FastOr(chunks...), nil
}

// SeekInBitmap - returns value in bitmap which is >= n
func SeekInBitmap64(m *roaring64.Bitmap, n uint64) (found uint64, ok bool) {
	if m.IsEmpty() {
		return 0, false
	}
	if n == 0 {
		return m.Minimum(), true
	}
	searchRank := m.Rank(n - 1)
	if searchRank >= m.GetCardinality() {
		return 0, false
	}
	found, _ = m.Select(searchRank)
	return found, true
}

func Walk(c kv.Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

type ToBitmap interface {
	ToBitmap() (*roaring64.Bitmap, error)
}

func ToIter(it roaring64.IntIterable64) *ToIterInterface { return &ToIterInterface{it: it} }

type ToIterInterface struct{ it roaring64.IntIterable64 }

func (i *ToIterInterface) HasNext() bool         { return i.it.HasNext() }
func (i *ToIterInterface) Next() (uint64, error) { return i.it.Next(), nil }
