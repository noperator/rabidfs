package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const retrySleep = 200 * time.Millisecond

type Manager struct {
	buckets       []*Bucket
	bucketsMutex  sync.RWMutex
	next          uint32 // Round-robin cursor (atomic)
	repairMutex   sync.Mutex
	repairRunning bool
}

func NewManager(bucketURLs []string) (*Manager, error) {
	if len(bucketURLs) == 0 {
		return nil, fmt.Errorf("no bucket URLs provided")
	}

	manager := &Manager{
		buckets: make([]*Bucket, 0, len(bucketURLs)),
	}

	for _, url := range bucketURLs {
		bucket, err := NewBucket(url)
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket for URL %s: %w", url, err)
		}
		manager.buckets = append(manager.buckets, bucket)
	}

	rand.Seed(time.Now().UnixNano())

	return manager, nil
}

func (m *Manager) GetAvailableBuckets() []*Bucket {
	m.bucketsMutex.RLock()
	defer m.bucketsMutex.RUnlock()

	availableBuckets := make([]*Bucket, 0, len(m.buckets))
	for _, bucket := range m.buckets {
		if bucket.IsAvailable() {
			availableBuckets = append(availableBuckets, bucket)
		}
	}

	return availableBuckets
}

// Return next available bucket in round-robin fashion
func (m *Manager) nextBucket() (*Bucket, error) {
	avail := m.GetAvailableBuckets()
	if len(avail) == 0 {
		return nil, fmt.Errorf("no available buckets")
	}

	idx := int(atomic.AddUint32(&m.next, 1)-1) % len(avail)
	return avail[idx], nil
}

// Puts chunk in random bucket
func (m *Manager) PutChunk(data []byte) (string, string, string, error) {
	tried := make(map[string]struct{})

	for {
		bucket, err := m.nextBucket()
		if err != nil {
			return "", "", "", fmt.Errorf("no available buckets: %w", err)
		}

		if _, seen := tried[bucket.GetURL()]; seen {
			if len(tried) == m.GetBucketCount() {
				return "", "", "", fmt.Errorf("all buckets failed")
			}
			continue
		}
		tried[bucket.GetURL()] = struct{}{}

		key := uuid.New().String()
		etag, err := bucket.PutObject(key, data)
		if err == nil {
			return key, bucket.GetURL(), etag, nil
		}

		bucket.SetAvailable(false)
		fmt.Printf("Bucket %s failed: %v (marked unavailable)\n", bucket.GetURL(), err)

		// Short pause to avoid hammering
		time.Sleep(retrySleep)
	}
}

func (m *Manager) GetChunk(key string, bucketURL string) ([]byte, error) {
	m.bucketsMutex.RLock()
	var bucket *Bucket
	for _, b := range m.buckets {
		if b.GetURL() == bucketURL && b.IsAvailable() {
			bucket = b
			break
		}
	}
	m.bucketsMutex.RUnlock()

	if bucket == nil {
		return nil, fmt.Errorf("bucket not found or not available: %s", bucketURL)
	}

	data, err := bucket.GetObject(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk from bucket: %w", err)
	}

	return data, nil
}

func (m *Manager) VerifyChunk(key string, bucketURL string, expectedETag string) (bool, error) {
	m.bucketsMutex.RLock()
	var bucket *Bucket
	for _, b := range m.buckets {
		if b.GetURL() == bucketURL && b.IsAvailable() {
			bucket = b
			break
		}
	}
	m.bucketsMutex.RUnlock()

	if bucket == nil {
		return false, fmt.Errorf("bucket not found or not available: %s", bucketURL)
	}

	etag, err := bucket.HeadObject(key)
	if err != nil {
		return false, fmt.Errorf("failed to get chunk metadata from bucket: %w", err)
	}

	return etag == expectedETag, nil
}

func (m *Manager) GetBucketCount() int {
	m.bucketsMutex.RLock()
	defer m.bucketsMutex.RUnlock()

	return len(m.buckets)
}

func (m *Manager) DeleteChunk(key, bucketURL string) error {
	m.bucketsMutex.RLock()
	var b *Bucket
	for _, bb := range m.buckets {
		if bb.GetURL() == bucketURL && bb.IsAvailable() {
			b = bb
			break
		}
	}
	m.bucketsMutex.RUnlock()
	if b == nil {
		return fmt.Errorf("bucket not found or not available: %s", bucketURL)
	}
	return b.DeleteObject(key)
}
