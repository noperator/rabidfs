package repair

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/noperator/rabidfs/pkg/crypto"
	"github.com/noperator/rabidfs/pkg/erasure"
	"github.com/noperator/rabidfs/pkg/manifest"
	"github.com/noperator/rabidfs/pkg/storage"
)

type Worker struct {
	fsManifest *manifest.Manifest
	stor       *storage.Manager
	ticker     *time.Ticker
	quit       chan struct{}
}

func NewWorker(m *manifest.Manifest, s *storage.Manager, every time.Duration) *Worker {
	return &Worker{
		fsManifest: m,
		stor:       s,
		ticker:     time.NewTicker(every),
		quit:       make(chan struct{}),
	}
}

func (w *Worker) Start() {
	go func() {
		for {
			select {
			case <-w.ticker.C:
				w.runPass()
			case <-w.quit:
				return
			}
		}
	}()
}

func (w *Worker) Stop() { close(w.quit); w.ticker.Stop() }

func (w *Worker) runPass() {
	files := w.fsManifest.ListFiles()
	fmt.Printf("[REPAIR] Starting repair pass for %d files\n", len(files))
	for _, f := range files {
		fmt.Printf("[REPAIR] Processing file: %s with %d chunks\n", f.Name, len(f.Chunks))
		w.repairFile(f)
	}
	fmt.Printf("[REPAIR] Repair pass completed\n")
}

func (w *Worker) repairFile(fi manifest.FileInfo) {
	total := fi.DataShards + fi.ParityShards
	fmt.Printf("[REPAIR] File %s: %d+%d shards, %d chunks total\n", fi.Name, fi.DataShards, fi.ParityShards, len(fi.Chunks))

	// Group chunks by stripe
	stripeMap := make(map[int][]manifest.ChunkInfo)
	for _, c := range fi.Chunks {
		stripeIndex := c.Index / total
		stripeMap[stripeIndex] = append(stripeMap[stripeIndex], c)
		fmt.Printf("[REPAIR] Chunk %s: index=%d, stripe=%d, bucket=%s\n", c.UUID, c.Index, stripeIndex, c.BucketURL)
	}

	for stripeIndex, chunks := range stripeMap {
		fmt.Printf("[REPAIR] Processing stripe %d with %d chunks\n", stripeIndex, len(chunks))

		// Create a full stripe array
		stripe := make([]manifest.ChunkInfo, total)
		for _, c := range chunks {
			localIndex := c.Index % total
			stripe[localIndex] = c
		}

		w.verifyAndHeal(fi, stripe)
	}
}

func (w *Worker) verifyAndHeal(fi manifest.FileInfo, stripe []manifest.ChunkInfo) {
	fmt.Printf("[REPAIR] verifyAndHeal called for file %s\n", fi.Name)
	var missingIdx []int
	missing := make([]bool, fi.DataShards+fi.ParityShards)
	shards := make([][]byte, fi.DataShards+fi.ParityShards)

	// Initially assume all shards missing (will prove otherwise below)
	for i := range missing {
		missing[i] = true
	}

	for _, c := range stripe {
		if len(c.UUID) == 0 {
			continue
		}

		ok, err := w.stor.VerifyChunk(c.UUID, c.BucketURL, c.ETag)
		fmt.Printf("[REPAIR] VerifyChunk for %s in %s: ok=%v, err=%v\n", c.UUID, c.BucketURL, ok, err)
		if !ok {
			fmt.Printf("[REPAIR] Chunk %s is missing/corrupted, adding to repair list\n", c.UUID)
			missingIdx = append(missingIdx, c.Index)
			continue
		}

		data, err := w.stor.GetChunk(c.UUID, c.BucketURL)
		fmt.Printf("[REPAIR] GetChunk for %s: err=%v, data_len=%d\n", c.UUID, err, len(data))
		if err != nil {
			fmt.Printf("[REPAIR] GetChunk failed for %s, adding to repair list\n", c.UUID)
			missingIdx = append(missingIdx, c.Index)
			continue
		}

		shardNonce := deriveShardNonce(fi.EncryptionNonce, c.Index)
		decrypted, err := crypto.Decrypt(data, w.fsManifest.GetEncryptionKey(), shardNonce)
		fmt.Printf("[REPAIR] Decrypt for %s: err=%v, decrypted_len=%d\n", c.UUID, err, len(decrypted))
		if err != nil {
			fmt.Printf("[REPAIR] Decryption failed for %s, adding to repair list\n", c.UUID)
			missingIdx = append(missingIdx, c.Index)
			continue
		}

		shards[c.Index] = decrypted
		missing[c.Index] = false
	}

	// Check if we have enough chunks to reconstruct
	missingCount := 0
	for _, m := range missing {
		if m {
			missingCount++
		}
	}

	fmt.Printf("[REPAIR] Missing chunks: %d, parity shards: %d, missing indices: %v\n", missingCount, fi.ParityShards, missingIdx)

	if missingCount == 0 {
		fmt.Printf("[REPAIR] No missing chunks, nothing to repair\n")
		return
	}

	if missingCount > fi.ParityShards {
		fmt.Printf("[REPAIR] Too many missing chunks (%d > %d), cannot recover\n", missingCount, fi.ParityShards)
		return
	}

	fmt.Printf("[REPAIR] Starting reconstruction of %d missing chunks\n", missingCount)

	enc, err := erasure.NewEncoder(fi.DataShards, fi.ParityShards)
	if err != nil {
		fmt.Printf("[REPAIR] Failed to create erasure encoder: %v\n", err)
		return
	}
	fmt.Printf("[REPAIR] Created erasure encoder: %d+%d\n", fi.DataShards, fi.ParityShards)

	// Calculate correct stripe data size
	stripeIndex := stripe[0].Index / (fi.DataShards + fi.ParityShards)
	totalStripes := (len(fi.Chunks) + fi.DataShards + fi.ParityShards - 1) / (fi.DataShards + fi.ParityShards)

	var stripeDataSize int64
	if stripeIndex == totalStripes-1 {
		stripeDataSize = fi.Size % fi.ChunkSize
		if stripeDataSize == 0 && fi.Size > 0 {
			stripeDataSize = fi.ChunkSize
		}
	} else {
		stripeDataSize = fi.ChunkSize
	}

	fmt.Printf("[REPAIR] Stripe %d: calculated data size %d bytes (total file size: %d)\n", stripeIndex, stripeDataSize, fi.Size)

	// Reconstruct missing shards
	fmt.Printf("[REPAIR] Attempting Reed-Solomon reconstruction...\n")
	_, err = enc.ReconstructWithMissing(shards, missing, stripeDataSize)
	if err != nil {
		fmt.Printf("[REPAIR] Reed-Solomon reconstruction failed: %v\n", err)
		return // unable to reconstruct
	}
	fmt.Printf("[REPAIR] Reed-Solomon reconstruction successful\n")

	// Re-upload missing shards and update manifest
	fmt.Printf("[REPAIR] Re-uploading %d missing shards...\n", len(missingIdx))
	for _, idx := range missingIdx {
		fmt.Printf("[REPAIR] Processing missing shard at index %d\n", idx)
		nonce := deriveShardNonce(fi.EncryptionNonce, idx)
		encShard, err := crypto.Encrypt(shards[idx], w.fsManifest.GetEncryptionKey(), nonce)
		if err != nil {
			fmt.Printf("[REPAIR] Encryption failed for shard %d: %v\n", idx, err)
			continue
		}
		fmt.Printf("[REPAIR] Encrypted shard %d, size: %d\n", idx, len(encShard))

		uuid, bucket, etag, err := w.stor.PutChunk(encShard)
		if err != nil {
			fmt.Printf("[REPAIR] PutChunk failed for shard %d: %v\n", idx, err)
			continue
		}
		fmt.Printf("[REPAIR] Successfully uploaded repaired shard %d: UUID=%s, bucket=%s, etag=%s\n", idx, uuid, bucket, etag)
		stripe[idx] = manifest.ChunkInfo{
			UUID:       uuid,
			BucketURL:  bucket,
			ETag:       etag,
			Index:      idx,
			Size:       int64(len(encShard)),
			IsData:     idx < fi.DataShards,
			LastVerify: time.Now(),
		}
	}

	fi.ModTime = time.Now()
	fmt.Printf("[REPAIR] Updating manifest with repaired chunks\n")
	copy(fi.Chunks[getStripeBase(fi, stripe[0].Index):], stripe)
	err = w.fsManifest.UpdateFile(fi)
	if err != nil {
		fmt.Printf("[REPAIR] Failed to update manifest: %v\n", err)
	} else {
		fmt.Printf("[REPAIR] Manifest updated successfully\n")
	}
}

func getStripeBase(fi manifest.FileInfo, idx int) int {
	stripeWidth := fi.DataShards + fi.ParityShards
	return (idx / stripeWidth) * stripeWidth
}

func deriveShardNonce(fileNonce []byte, shardID int) []byte {
	nonce := make([]byte, crypto.NonceSize)
	copy(nonce, fileNonce)
	binary.BigEndian.PutUint32(nonce[crypto.NonceSize-4:], uint32(shardID))
	return nonce
}
