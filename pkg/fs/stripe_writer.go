package fs

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/noperator/rabidfs/pkg/crypto"
	"github.com/noperator/rabidfs/pkg/erasure"
	"github.com/noperator/rabidfs/pkg/manifest"
	"github.com/noperator/rabidfs/pkg/storage"
)

type stripeWriter struct {
	chunkSize     int64
	buf           bytes.Buffer
	nextIdx       int
	fi            manifest.FileInfo
	enc           *erasure.Encoder
	stor          *storage.Manager
	key           []byte
	manifest      *manifest.Manifest
	partialLoaded bool
}

// Fetch last partial stripe of a file
func preloadTail(fi manifest.FileInfo, stor *storage.Manager, key []byte) ([]byte, error) {
	if fi.Size == 0 || fi.Size%fi.ChunkSize == 0 {
		return nil, nil
	}

	// Calculate the offset and size of the tail
	tailOff := fi.Size - fi.Size%fi.ChunkSize
	tailSize := fi.Size % fi.ChunkSize

	return readAt(fi, tailOff, tailSize, stor, key)
}

func newStripeWriter(fi manifest.FileInfo, stor *storage.Manager, key []byte, m *manifest.Manifest) *stripeWriter {
	enc, _ := erasure.NewEncoder(fi.DataShards, fi.ParityShards)

	// Calculate next stripe index based on existing chunks
	// This ensures appends continue correct stripe sequence
	nextIdx := 0
	stripeWidth := fi.DataShards + fi.ParityShards
	if len(fi.Chunks) > 0 {
		nextIdx = len(fi.Chunks) / stripeWidth
	}

	sw := &stripeWriter{
		chunkSize: fi.ChunkSize,
		fi:        fi,
		enc:       enc,
		stor:      stor,
		key:       key,
		manifest:  m,
		nextIdx:   nextIdx,
	}

	// Check for partial stripe at end of file
	if fi.Size > 0 && fi.Size%fi.ChunkSize != 0 {
		tail, err := preloadTail(fi, stor, key)
		if err == nil && len(tail) > 0 {
			sw.buf.Write(tail)
			sw.partialLoaded = true

			// Drop the old tail shards from the manifest
			stripes := (len(fi.Chunks) + stripeWidth - 1) / stripeWidth
			if stripes > 0 {
				lastBase := (stripes - 1) * stripeWidth
				if fi.Size%fi.ChunkSize != 0 {
					fi.Chunks = fi.Chunks[:lastBase]
					fi.Size -= fi.Size % fi.ChunkSize
					m.UpdateFile(fi)
					sw.fi = fi
					sw.nextIdx = lastBase / stripeWidth
				}
			}
		}
	}

	return sw
}

func (w *stripeWriter) Write(p []byte) (int, error) {
	n, _ := w.buf.Write(p)

	// Process full stripes only
	for int64(w.buf.Len()) >= w.chunkSize {
		stripe := make([]byte, w.chunkSize)
		w.buf.Read(stripe)
		if err := w.flushStripe(stripe); err != nil {
			return n, err
		}
	}

	return n, nil
}

// Process data smaller than a full stripe
func (w *stripeWriter) flushPartialStripe(data []byte) error {
	orig := len(data)

	// Pad data to minimum size required by erasure encoder
	minSize := w.fi.DataShards
	if orig < minSize {
		paddedData := make([]byte, minSize)
		copy(paddedData, data)
		data = paddedData
	}

	// Split data into shards
	shards, _ := w.enc.Split(data)
	stripeWidth := w.fi.DataShards + w.fi.ParityShards
	for i, shard := range shards {
		idx := w.nextIdx*stripeWidth + i
		nonce := deriveShardNonce(w.fi.EncryptionNonce, idx)
		encShard, _ := crypto.Encrypt(shard, w.key, nonce)

		uuid, bucket, etag, err := w.stor.PutChunk(encShard)
		if err != nil {
			return err
		}

		w.fi.Chunks = append(w.fi.Chunks, manifest.ChunkInfo{
			UUID: uuid, BucketURL: bucket, ETag: etag,
			Index: idx, Size: int64(len(encShard)),
			IsData: i < w.fi.DataShards, LastVerify: w.fi.ModTime,
		})
	}

	w.fi.Size += int64(orig)
	w.fi.ModTime = time.Now()
	w.nextIdx++
	return w.manifest.UpdateFile(w.fi)
}

func (w *stripeWriter) flushStripe(data []byte) error {
	shards, _ := w.enc.Split(data)
	stripeWidth := w.fi.DataShards + w.fi.ParityShards
	for i, shard := range shards {
		idx := w.nextIdx*stripeWidth + i
		nonce := deriveShardNonce(w.fi.EncryptionNonce, idx)
		encShard, _ := crypto.Encrypt(shard, w.key, nonce)

		uuid, bucket, etag, err := w.stor.PutChunk(encShard)
		if err != nil {
			return err
		}

		w.fi.Chunks = append(w.fi.Chunks, manifest.ChunkInfo{
			UUID: uuid, BucketURL: bucket, ETag: etag,
			Index: idx, Size: int64(len(encShard)),
			IsData: i < w.fi.DataShards, LastVerify: w.fi.ModTime,
		})
	}
	w.fi.Size += w.chunkSize
	w.fi.ModTime = time.Now()
	w.nextIdx++
	return w.manifest.UpdateFile(w.fi)
}

// Flush any remaining data in buffer and close writer
func (w *stripeWriter) Close() error {
	if w.buf.Len() == 0 {
		return nil
	}

	data := make([]byte, w.buf.Len())
	copy(data, w.buf.Bytes()) // grab tail

	w.buf.Reset()

	return w.flushPartialStripe(data)
}

func deriveShardNonce(fileNonce []byte, shardID int) []byte {
	nonce := make([]byte, crypto.NonceSize)
	copy(nonce, fileNonce)
	binary.BigEndian.PutUint32(nonce[crypto.NonceSize-4:], uint32(shardID))
	return nonce
}

// Read portion of file at specified offset/size
func readAt(fi manifest.FileInfo, off, size int64, stor *storage.Manager, key []byte) ([]byte, error) {
	stripeWidth := fi.DataShards + fi.ParityShards
	stripesTotal := (len(fi.Chunks) + stripeWidth - 1) / stripeWidth

	var out bytes.Buffer
	for s := 0; s < stripesTotal; s++ {
		base := s * stripeWidth
		if base >= len(fi.Chunks) {
			break
		}

		// Determine how many shards we have in this stripe
		// (might be less than stripeWidth for the last stripe)
		count := stripeWidth
		if base+count > len(fi.Chunks) {
			count = len(fi.Chunks) - base
		}

		shards := make([][]byte, stripeWidth)
		missing := make([]bool, stripeWidth)

		// Mark all shards as missing initially
		for i := range missing {
			missing[i] = true
		}

		// Get all available chunks for this stripe
		for i := 0; i < count; i++ {
			c := fi.Chunks[base+i]
			buf, err := stor.GetChunk(c.UUID, c.BucketURL)
			if err != nil {
				continue
			}

			// Use the actual index stored in the manifest for decryption
			nonce := deriveShardNonce(fi.EncryptionNonce, c.Index)
			plain, err := crypto.Decrypt(buf, key, nonce)
			if err != nil {
				continue
			}

			// Map the shard to its position in the stripe
			shardPos := c.Index % stripeWidth
			shards[shardPos] = plain
			missing[shardPos] = false
		}

		enc, _ := erasure.NewEncoder(fi.DataShards, fi.ParityShards)

		// Calculate how much real data is in this stripe
		need := fi.Size - int64(out.Len())
		if need > fi.ChunkSize {
			need = fi.ChunkSize
		}

		block, err := enc.ReconstructWithMissing(shards, missing, need)
		if err != nil {
			return nil, err
		}

		out.Write(block)
	}

	// Slice to the caller's requested window
	if off >= int64(out.Len()) {
		return []byte{}, nil
	}

	end := off + size
	if end > int64(out.Len()) {
		end = int64(out.Len())
	}

	return out.Bytes()[off:end], nil
}
