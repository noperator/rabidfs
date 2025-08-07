package erasure

import (
	"bytes"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Reed-Solomon erasure encoder/decoder
type Encoder struct {
	enc          reedsolomon.Encoder
	dataShards   int
	parityShards int
	totalShards  int
	shardSize    int
	originalSize int64
}

func NewEncoder(dataShards, parityShards int) (*Encoder, error) {
	if dataShards <= 0 {
		return nil, fmt.Errorf("data shards must be positive")
	}
	if parityShards <= 0 {
		return nil, fmt.Errorf("parity shards must be positive")
	}

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon encoder: %w", err)
	}

	return &Encoder{
		enc:          enc,
		dataShards:   dataShards,
		parityShards: parityShards,
		totalShards:  dataShards + parityShards,
	}, nil
}

// Split data into shards
func (e *Encoder) Split(data []byte) ([][]byte, error) {
	e.originalSize = int64(len(data))
	shardSize := (len(data) + e.dataShards - 1) / e.dataShards
	e.shardSize = shardSize

	fmt.Printf("Erasure Split: data size=%d, data shards=%d, parity shards=%d, shard size=%d\n",
		len(data), e.dataShards, e.parityShards, shardSize)

	if len(data) < e.dataShards {
		fmt.Printf("Warning: data size (%d) is smaller than number of data shards (%d)\n",
			len(data), e.dataShards)
	}

	shards, err := e.enc.Split(data)
	if err != nil {
		fmt.Printf("Error splitting data: %v\n", err)
		return nil, fmt.Errorf("failed to split data: %w", err)
	}
	fmt.Printf("Successfully split data into %d shards\n", len(shards))

	// Encode the parity shards
	err = e.enc.Encode(shards)
	if err != nil {
		fmt.Printf("Error encoding parity shards: %v\n", err)
		return nil, fmt.Errorf("failed to encode parity shards: %w", err)
	}
	fmt.Printf("Successfully encoded parity shards\n")

	return shards, nil
}

// Reconstruct original data from shards with some missing
func (e *Encoder) ReconstructWithMissing(shards [][]byte, missing []bool, originalSize int64) ([]byte, error) {
	if len(shards) != e.totalShards {
		return nil, fmt.Errorf("invalid number of shards: expected %d, got %d", e.totalShards, len(shards))
	}
	if len(missing) != e.totalShards {
		return nil, fmt.Errorf("invalid missing array length: expected %d, got %d", e.totalShards, len(missing))
	}

	missingCount := 0
	for _, m := range missing {
		if m {
			missingCount++
		}
	}

	if missingCount > e.parityShards {
		return nil, fmt.Errorf("too many missing shards: %d (max %d)", missingCount, e.parityShards)
	}

	// Reconstruct any missing shards
	err := e.enc.ReconstructData(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct shards: %w", err)
	}

	// Join data shards to get original data
	var buf bytes.Buffer
	err = e.enc.Join(&buf, shards, int(originalSize))
	if err != nil {
		return nil, fmt.Errorf("failed to join shards: %w", err)
	}
	data := buf.Bytes()

	return data, nil
}
