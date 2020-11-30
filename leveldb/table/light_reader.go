// Copyright (c) 2020, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"io"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// onetimeBlockIter is identical to the blockIter, while the main difference
// is it's more resource efficient. Resource effectiveness is mainly reflected
// that:
//
// The resource used by the iterator will all be released if the iterator is
// already exhausted or some errors have occurred.
//
// So this also means that this iterator is for one-time use, it's mainly
// used in some special cases like compaction.
//
// The iterator is safe the concurrent usage.
type onetimeBlockIter struct {
	lock sync.Mutex
	ierr error
	iter *blockIter // nil means the iterator is released
}

func (i *onetimeBlockIter) First() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	valid := i.iter.First()
	if !valid {
		i.ierr = i.iter.Error()
		i.iter = nil
	}
	return valid
}

func (i *onetimeBlockIter) Last() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	valid := i.iter.Last()
	if !valid {
		i.ierr = i.iter.Error()
		i.iter = nil
	}
	return valid
}

func (i *onetimeBlockIter) Seek(key []byte) bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	return i.iter.Seek(key)
}

func (i *onetimeBlockIter) Next() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	valid := i.iter.Next()
	if !valid {
		i.ierr = i.iter.Error()
		i.iter = nil
	}
	return valid
}

func (i *onetimeBlockIter) Prev() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	valid := i.iter.Prev()
	if !valid {
		i.ierr = i.iter.Error()
		i.iter = nil
	}
	return valid
}

func (i *onetimeBlockIter) Key() []byte {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return nil
	}
	return i.iter.Key()
}

func (i *onetimeBlockIter) Value() []byte {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return nil
	}
	return i.iter.Value()
}

func (i *onetimeBlockIter) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return
	}
	i.iter.Release()
	i.iter = nil
}

func (i *onetimeBlockIter) SetReleaser(releaser util.Releaser) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return
	}
	i.iter.SetReleaser(releaser)
}

func (i *onetimeBlockIter) Valid() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return false
	}
	return i.iter.Valid()
}

func (i *onetimeBlockIter) Error() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return i.ierr
	}
	return i.iter.Error()
}

type position int

const (
	posUninitialized position = iota
	posFirst
	posLast
)

// lightIndexIter is identical to the indexIter, while the main difference
// is it's more resource efficient. Resource effectiveness is mainly reflected
// in two aspects:
//
// - the resource won't be allocated only if the first entry in the iterator
// 	 is accessed. E.g. the index block won't be loaded
// - the resource used by the iterator will all be released if the iterator
//   is already exhausted.
//
// So this also means that this iterator is for one-time use, it's mainly
// used in some special cases like compaction.
type lightIndexIter struct {
	lock      sync.Mutex
	iter      *onetimeBlockIter
	tr        *LightReader
	slice     *util.Range
	rel       util.Releaser
	fillCache bool

	// The min and max are always available if the
	// underlying iterator is not initialized.
	min, max []byte
	pos      position
	err      error
}

// initialize initializes the underlying index block iterator.
// The lock is assumed to be held already.
func (i *lightIndexIter) initialize() error {
	indexBlock, rel, err := i.tr.reader.getIndexBlock(i.fillCache)
	if err != nil {
		return err
	}
	i.iter = &onetimeBlockIter{iter: i.tr.reader.newBlockIter(indexBlock, rel, i.slice, true)}
	if i.rel != nil {
		i.iter.SetReleaser(i.rel)
	}
	return nil
}

func (i *lightIndexIter) First() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		i.pos = posFirst
		return true
	}
	return i.iter.First()
}

func (i *lightIndexIter) Last() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		i.pos = posLast
		return true
	}
	return i.iter.Last()
}

func (i *lightIndexIter) Seek(key []byte) (res bool) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		if err := i.initialize(); err != nil {
			i.err = err
			return false
		}
	}
	return i.iter.Seek(key)
}

func (i *lightIndexIter) Next() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		if err := i.initialize(); err != nil {
			i.err = err
			return false
		}
		// Move the iterator to the real position
		// before call next.
		if i.pos == posFirst {
			i.iter.First()
		} else if i.pos == posLast {
			i.iter.Last()
		}
		i.pos = posUninitialized
	}
	return i.iter.Next()
}

func (i *lightIndexIter) Prev() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		if err := i.initialize(); err != nil {
			i.err = err
			return false
		}
		// Move the iterator to the real position
		// before call next.
		if i.pos == posFirst {
			i.iter.First()
		} else if i.pos == posLast {
			i.iter.Last()
		}
		i.pos = posUninitialized
	}
	return i.iter.Prev()
}

func (i *lightIndexIter) Key() []byte {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		switch i.pos {
		case posFirst:
			return i.min
		case posLast:
			return i.max
		default:
			return nil // todo perhaps panic here?
		}
	}
	return i.iter.Key()
}

func (i *lightIndexIter) Value() []byte {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		if err := i.initialize(); err != nil {
			i.err = err
			return nil
		}
		// Move the iterator to the real position
		// before call next.
		if i.pos == posFirst {
			i.iter.First()
		} else if i.pos == posLast {
			i.iter.Last()
		}
		i.pos = posUninitialized
	}
	return i.iter.Value()
}

func (i *lightIndexIter) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return
	}
	i.iter.Release()
}

func (i *lightIndexIter) SetReleaser(releaser util.Releaser) {
	i.lock.Lock()
	defer i.lock.Unlock()

	// Cache the releaser if the iter is not initialized yet
	if i.iter == nil {
		i.rel = releaser
		return
	}
	i.iter.SetReleaser(releaser)
}

func (i *lightIndexIter) Valid() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.iter == nil {
		return true
	}
	return i.iter.Valid()
}

func (i *lightIndexIter) Error() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	// Return nothing if the iterator is not initialized
	if i.iter == nil {
		return nil
	}
	// Return the cached error if it's not nil
	if i.err != nil {
		return i.err
	}
	return i.iter.Error()
}

func (i *lightIndexIter) Get() iterator.Iterator {
	value := i.Value()
	if value == nil {
		return nil
	}
	dataBH, n := decodeBlockHandle(value)
	if n == 0 {
		return iterator.NewEmptyIterator(i.tr.reader.newErrCorruptedBH(i.tr.reader.indexBH, "bad data block handle"))
	}
	var slice *util.Range
	if i.slice != nil && (i.iter.iter.isFirst() || i.iter.iter.isLast()) {
		slice = i.slice
	}
	return i.tr.reader.getDataIterErr(dataBH, slice, i.tr.reader.verifyChecksum, i.fillCache)
}

// LightReader is identical to the Reader, while the main difference is it's
// more resource efficient. It's mainly used in some special cases like compaction.
type LightReader struct {
	reader   *Reader
	min, max []byte
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
// If filtered is true then the nearest 'block' will be checked against
// 'filter data' (if present) and will immediately return ErrNotFound if
// 'filter data' indicates that such pair doesn't exist.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *LightReader) Find(key []byte, filtered bool, ro *opt.ReadOptions) (rkey, value []byte, err error) {
	return r.reader.find(key, filtered, ro, false)
}

// FindKey finds key that is greater than or equal to the given key.
// It returns ErrNotFound if the table doesn't contain such key.
// If filtered is true then the nearest 'block' will be checked against
// 'filter data' (if present) and will immediately return ErrNotFound if
// 'filter data' indicates that such key doesn't exist.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *LightReader) FindKey(key []byte, filtered bool, ro *opt.ReadOptions) (rkey []byte, err error) {
	rkey, _, err = r.reader.find(key, filtered, ro, true)
	return
}

// Get gets the value for the given key. It returns errors.ErrNotFound
// if the table does not contain the key.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *LightReader) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	return r.reader.Get(key, ro)
}

// OffsetOf returns approximate offset for the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (r *LightReader) OffsetOf(key []byte) (offset int64, err error) {
	return r.reader.OffsetOf(key)
}

// Release implements util.Releaser.
// It also close the file if it is an io.Closer.
func (r *LightReader) Release() {
	r.reader.Release()
	r.min, r.max = nil, nil
}

// NewIterator creates a light iterator from the table. The returned
// iterator is the wrapper of the normal iterator while the main difference
// is it's more resource efficient. The meta data won't be loaded only
// if necessary. The resource will be released if the iterator is ever
// exhausted(both forward or backward iteration) or any error has occurred.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// table. And a nil Range.Limit is treated as a key after all keys in
// the table.
//
// WARNING: Any slice returned by iterator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The returned iterator is not safe for concurrent use and should be released
// after use.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (r *LightReader) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	r.reader.mu.RLock()
	defer r.reader.mu.RUnlock()

	if r.reader.err != nil {
		return iterator.NewEmptyIterator(r.reader.err)
	}
	var (
		min, max []byte
		init     bool
	)
	if slice == nil || (slice.Start == nil && slice.Limit == nil) {
		init = true
	} else {
		if slice.Start == nil {
			min = append([]byte{}, r.min...)
		}
		if slice.Limit == nil {
			max = append([]byte{}, r.max...)
		}
	}
	index := &lightIndexIter{
		tr:        r,
		slice:     slice,
		fillCache: !ro.GetDontFillCache(),
		min:       min,
		max:       max,
		pos:       posUninitialized,
	}
	if init {
		if err := index.initialize(); err != nil {
			return iterator.NewEmptyIterator(err)
		}
	}
	return iterator.NewIndexedIterator(index, opt.GetStrict(r.reader.o, ro, opt.StrictReader))
}

// NewLightReader creates a new initialized light table reader for the file.
// The fi, cache and bpool is optional and can be nil.
//
// The min and max refers to the minimal and maximum keys in the tables. They
// are always expected to be non-nil(equal is allowed).
//
// The returned table reader instance is safe for concurrent use.
func NewLightReader(f io.ReaderAt, size int64, min, max []byte, fd storage.FileDesc, cache *cache.NamespaceGetter, bpool *util.BufferPool, o *opt.Options) (*LightReader, error) {
	reader, err := NewReader(f, size, fd, cache, bpool, o, false)
	if err != nil {
		return nil, err
	}
	return &LightReader{
		reader: reader,
		min:    append([]byte{}, min...),
		max:    append([]byte{}, max...),
	}, nil
}
