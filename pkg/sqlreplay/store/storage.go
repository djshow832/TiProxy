// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"io"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

var _ io.WriteCloser = (*StorageWriter)(nil)

type StorageWriter struct {
	writer storage.ExternalFileWriter
}

func (s *StorageWriter) Write(p []byte) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return s.writer.Write(ctx, p)
}

func (s *StorageWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	return s.writer.Close(ctx)
}

func NewStorageWriter(writer storage.ExternalFileWriter) *StorageWriter {
	return &StorageWriter{writer: writer}
}

// RetryableStorage retries when the token expires. It is not thread-safe.
type RetryableStorage struct {
	storage.ExternalStorage
	path string
	lg   *zap.Logger
}

func NewStorage(path string, lg *zap.Logger) (storage.ExternalStorage, error) {
	s := &RetryableStorage{
		path: path,
		lg:   lg,
	}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *RetryableStorage) open() error {
	backend, err := storage.ParseBackend(s.path, &storage.BackendOptions{})
	if err != nil {
		s.lg.Info("parse backend failed", zap.NamedError("parse_err", err))
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	externalStorage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{})
	if err != nil {
		s.lg.Info("new external storage failed", zap.NamedError("new_err", err))
		return err
	}
	s.ExternalStorage = externalStorage
	s.lg.Info("storage opened", zap.String("path", s.path))
	return nil
}

func (s *RetryableStorage) retry(fn func() error) error {
	err := fn()
	if err == nil || !strings.Contains(err.Error(), "ExpiredToken") {
		return err
	}
	err = s.open()
	s.lg.Info("token expired, reopen", zap.NamedError("reopen_err", err))
	if err == nil {
		return fn()
	}
	return err
}

func (s *RetryableStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	return s.retry(func() error {
		return s.ExternalStorage.WriteFile(ctx, name, data)
	})
}

func (s *RetryableStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	var data []byte
	err := s.retry(func() error {
		var err error
		data, err = s.ExternalStorage.ReadFile(ctx, name)
		return err
	})
	return data, err
}

func (s *RetryableStorage) FileExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	err := s.retry(func() error {
		var err error
		exists, err = s.ExternalStorage.FileExists(ctx, name)
		return err
	})
	return exists, err
}

func (s *RetryableStorage) DeleteFile(ctx context.Context, name string) error {
	return s.retry(func() error {
		return s.ExternalStorage.DeleteFile(ctx, name)
	})
}

func (s *RetryableStorage) Open(ctx context.Context, path string, option *storage.ReaderOption) (storage.ExternalFileReader, error) {
	var reader storage.ExternalFileReader
	err := s.retry(func() error {
		var err error
		reader, err = s.ExternalStorage.Open(ctx, path, option)
		s.lg.Info("opening", zap.String("path", path), zap.Error(err))
		return err
	})
	return reader, err
}

func (s *RetryableStorage) DeleteFiles(ctx context.Context, names []string) error {
	return s.retry(func() error {
		return s.ExternalStorage.DeleteFiles(ctx, names)
	})
}

func (s *RetryableStorage) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(path string, size int64) error) error {
	return s.retry(func() error {
		return s.ExternalStorage.WalkDir(ctx, opt, fn)
	})
}

func (s *RetryableStorage) Create(ctx context.Context, path string, option *storage.WriterOption) (storage.ExternalFileWriter, error) {
	var writer storage.ExternalFileWriter
	err := s.retry(func() error {
		var err error
		writer, err = s.ExternalStorage.Create(ctx, path, option)
		return err
	})
	return writer, err
}

func (s *RetryableStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	return s.retry(func() error {
		return s.ExternalStorage.Rename(ctx, oldFileName, newFileName)
	})
}
