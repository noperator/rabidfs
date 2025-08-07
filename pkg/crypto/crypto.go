package crypto

import (
	"crypto/rand"
	"fmt"
	"io"

	"golang.org/x/crypto/nacl/secretbox"
)

const (
	NonceSize = 24
	KeySize   = 32
)

// Generate a random nonce for encryption
func GenerateNonce() ([]byte, error) {
	nonce := make([]byte, NonceSize)
	_, err := io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	return nonce, nil
}

// Encrypt data using NaCl's secretbox
func Encrypt(data []byte, key []byte, nonce []byte) ([]byte, error) {
	fmt.Printf("Encrypting %d bytes of data\n", len(data))
	fmt.Printf("Key length: %d, Nonce length: %d\n", len(key), len(nonce))

	if len(key) != KeySize {
		fmt.Printf("Invalid key size: expected %d, got %d\n", KeySize, len(key))
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", KeySize, len(key))
	}
	if len(nonce) != NonceSize {
		fmt.Printf("Invalid nonce size: expected %d, got %d\n", NonceSize, len(nonce))
		return nil, fmt.Errorf("invalid nonce size: expected %d, got %d", NonceSize, len(nonce))
	}

	var secretKey [KeySize]byte
	var secretNonce [NonceSize]byte
	copy(secretKey[:], key)
	copy(secretNonce[:], nonce)

	fmt.Printf("Performing encryption...\n")
	encrypted := secretbox.Seal(nil, data, &secretNonce, &secretKey)
	fmt.Printf("Encryption successful, encrypted size: %d bytes\n", len(encrypted))
	return encrypted, nil
}

// Decrypt data using NaCl's secretbox
func Decrypt(encrypted []byte, key []byte, nonce []byte) ([]byte, error) {
	fmt.Printf("Decrypting %d bytes of data\n", len(encrypted))
	fmt.Printf("Key length: %d, Nonce length: %d\n", len(key), len(nonce))

	if len(key) != KeySize {
		fmt.Printf("Invalid key size: expected %d, got %d\n", KeySize, len(key))
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", KeySize, len(key))
	}
	if len(nonce) != NonceSize {
		fmt.Printf("Invalid nonce size: expected %d, got %d\n", NonceSize, len(nonce))
		return nil, fmt.Errorf("invalid nonce size: expected %d, got %d", NonceSize, len(nonce))
	}

	var secretKey [KeySize]byte
	var secretNonce [NonceSize]byte
	copy(secretKey[:], key)
	copy(secretNonce[:], nonce)

	fmt.Printf("Performing decryption...\n")
	decrypted, ok := secretbox.Open(nil, encrypted, &secretNonce, &secretKey)
	if !ok {
		fmt.Printf("Decryption failed\n")
		return nil, fmt.Errorf("decryption failed")
	}
	fmt.Printf("Decryption successful, decrypted size: %d bytes\n", len(decrypted))
	return decrypted, nil
}
