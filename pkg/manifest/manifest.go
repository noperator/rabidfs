package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type ChunkInfo struct {
	UUID       string    `json:"uuid"`
	BucketURL  string    `json:"bucket_url"`
	Index      int       `json:"index"`
	Size       int64     `json:"size"`    // Size of the chunk in bytes
	IsData     bool      `json:"is_data"` // Whether this is a data chunk or a parity chunk
	ETag       string    `json:"etag"`    // ETag (MD5) of the chunk for verification
	LastVerify time.Time `json:"last_verify"`
}

type FileInfo struct {
	Name            string      `json:"name"`
	Size            int64       `json:"size"` // Size of the file in bytes
	Mode            uint32      `json:"mode"` // File mode/permissions
	ModTime         time.Time   `json:"mod_time"`
	Chunks          []ChunkInfo `json:"chunks"`
	DataShards      int         `json:"data_shards"`
	ParityShards    int         `json:"parity_shards"`
	ChunkSize       int64       `json:"chunk_size"` // Size of each chunk in bytes
	EncryptionNonce []byte      `json:"encryption_nonce"`
}

type Manifest struct {
	EncryptionKey []byte              `json:"encryption_key"`
	Files         map[string]FileInfo `json:"files"`
	mutex         sync.RWMutex        `json:"-"`
	path          string              `json:"-"`
}

func NewManifest(path string) (*Manifest, error) {
	m := &Manifest{
		Files: make(map[string]FileInfo),
		path:  path,
	}

	if _, err := os.Stat(path); err == nil {
		if err := m.Load(); err != nil {
			return nil, fmt.Errorf("failed to load existing manifest: %w", err)
		}
	} else {
		m.EncryptionKey = make([]byte, 32) // 256-bit key
		if _, err := os.Stat("/dev/urandom"); err == nil {
			// Use /dev/urandom if available
			f, err := os.Open("/dev/urandom")
			if err != nil {
				return nil, fmt.Errorf("failed to open /dev/urandom: %w", err)
			}
			defer f.Close()
			if _, err := f.Read(m.EncryptionKey); err != nil {
				return nil, fmt.Errorf("failed to read from /dev/urandom: %w", err)
			}
		} else {
			// Fallback to crypto/rand
			fmt.Println("Using crypto/rand to generate encryption key")
			cryptoRand, err := os.Open("/dev/random")
			if err != nil {
				return nil, fmt.Errorf("failed to open /dev/random: %w", err)
			}
			defer cryptoRand.Close()
			if _, err := cryptoRand.Read(m.EncryptionKey); err != nil {
				return nil, fmt.Errorf("failed to read from /dev/random: %w", err)
			}
		}

		// Save the manifest with the new key
		if err := m.Save(); err != nil {
			return nil, fmt.Errorf("failed to save new manifest: %w", err)
		}
		fmt.Println("Generated and saved new encryption key")
	}

	return m, nil
}

func (m *Manifest) Load() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data, err := os.ReadFile(m.path)
	if err != nil {
		return fmt.Errorf("failed to read manifest file: %w", err)
	}

	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	return nil
}

func (m *Manifest) Save() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := os.WriteFile(m.path, data, 0600); err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}

	return nil
}

func (m *Manifest) GetFile(name string) (FileInfo, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	file, ok := m.Files[name]
	return file, ok
}

func (m *Manifest) AddFile(file FileInfo) error {
	m.mutex.Lock()
	m.Files[file.Name] = file
	m.mutex.Unlock()

	return m.Save()
}

func (m *Manifest) UpdateFile(file FileInfo) error {
	m.mutex.Lock()
	m.Files[file.Name] = file
	m.mutex.Unlock()

	return m.Save()
}

func (m *Manifest) RemoveFile(name string) error {
	m.mutex.Lock()
	delete(m.Files, name)
	m.mutex.Unlock()

	return m.Save()
}

func (m *Manifest) ListFiles() []FileInfo {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	files := make([]FileInfo, 0, len(m.Files))
	for _, file := range m.Files {
		files = append(files, file)
	}

	return files
}

func (m *Manifest) GetEncryptionKey() []byte {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.EncryptionKey
}
