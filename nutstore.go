// Copyright 2023 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package nutstore implements the blob.Store interface using NutsDB.
package nutstore

import (
	"context"
	"errors"
	"strings"

	"github.com/creachadair/ffs/blob"
	"github.com/xujiajun/nutsdb"
)

// Store implements the blob.Store interface using a NutsDB instance.
type Store struct {
	db     *nutsdb.DB
	bucket string
}

// Opener constructs a store backed by NutsDB from an address comprising a
// path, for use with the store package.
func Opener(_ context.Context, addr string) (blob.Store, error) {
	if bucket, path, ok := strings.Cut(addr, "@"); ok {
		return Open(path, &Options{Bucket: bucket})
	}
	return Open(addr, nil)
}

// Open creates a Store by opening the NutsDB specified by opts.
func Open(path string, opts *Options) (Store, error) {
	db, err := nutsdb.Open(nutsdb.DefaultOptions, nutsdb.WithDir(path))
	if err != nil {
		return Store{}, err
	}
	return Store{db: db, bucket: opts.bucket()}, nil
}

// Options provides options for opening a NutsDB instance.
type Options struct {
	Bucket string // use this bucket name
}

func (o *Options) bucket() string {
	if o == nil {
		return ""
	}
	return o.Bucket
}

// Close implements part of the blob.Store interface. It closes the underlying
// database instance and reports its result.
func (s Store) Close(_ context.Context) error {
	err := s.db.Close()
	if nutsdb.IsDBClosed(err) {
		return nil
	}
	return err
}

// Get implements part of blob.Store.
func (s Store) Get(_ context.Context, key string) ([]byte, error) {
	var data []byte
	if err := s.db.View(func(tx *nutsdb.Tx) error {
		e, err := tx.Get(s.bucket, []byte(key))
		if err == nil {
			data = append([]byte{}, e.Value...)
		}
		return err
	}); isNutFound(err) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

// Put implements part of blob.Store.
func (s Store) Put(_ context.Context, opts blob.PutOptions) error {
	return s.db.Update(func(tx *nutsdb.Tx) error {
		if !opts.Replace {
			_, err := tx.Get(s.bucket, []byte(opts.Key))
			if err == nil {
				return blob.KeyExists(opts.Key)
			} else if !isNutFound(err) {
				return err
			}
		}
		return tx.Put(s.bucket, []byte(opts.Key), opts.Data, nutsdb.Persistent)
	})
}

// Size implements part of blob.Store.
func (s Store) Size(_ context.Context, key string) (int64, error) {
	var size int64
	if err := s.db.View(func(tx *nutsdb.Tx) error {
		e, err := tx.Get(s.bucket, []byte(key))
		if err == nil {
			size = int64(len(e.Value))
		}
		return err
	}); isNutFound(err) {
		return 0, blob.KeyNotFound(key)
	} else if err != nil {
		return 0, err
	}
	return size, nil
}

// Delete implements part of blob.Store.
func (s Store) Delete(ctx context.Context, key string) error {
	return s.db.Update(func(tx *nutsdb.Tx) error {
		bk := []byte(key)
		if _, err := tx.Get(s.bucket, bk); isNutFound(err) {
			return blob.KeyNotFound(key)
		} else if err != nil {
			return err
		}
		return tx.Delete(s.bucket, bk)
	})
}

func isNutFound(err error) bool {
	return nutsdb.IsKeyNotFound(err) || errors.Is(err, nutsdb.ErrNotFoundKey) || nutsdb.IsBucketNotFound(err)
}

// List implements part of blob.Store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	err := s.db.View(func(tx *nutsdb.Tx) error {
		it := nutsdb.NewIterator(tx, s.bucket, nutsdb.IteratorOptions{})
		it.Seek([]byte(start))
		for {
			ok, err := it.SetNext()
			if !ok {
				return nil
			} else if err != nil {
				return err
			} else if err := ctx.Err(); err != nil {
				return err
			}
			if err := f(string(it.Entry().Key)); err != nil {
				return err
			}
		}
	})
	if errors.Is(err, blob.ErrStopListing) {
		return nil
	}
	return err
}

// Len implements part of blob.Store.
func (s Store) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.List(ctx, "", func(string) error {
		count++
		return nil
	})
	return count, err
}
