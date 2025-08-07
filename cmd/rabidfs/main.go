package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/noperator/rabidfs/pkg/fs"
)

func main() {
	manifestPath := flag.String("m", "manifest.json", "Path to the manifest file")
	bucketsPath := flag.String("b", "buckets.txt", "Path to the buckets file")
	dataShards := flag.Int("data-shards", 2, "Number of data shards for erasure coding")
	parityShards := flag.Int("parity-shards", 1, "Number of parity shards for erasure coding")
	repairInterval := flag.Duration("repair-interval", 5*time.Minute, "Interval between repair operations")
	flag.Parse()

	if *dataShards < 1 {
		fmt.Fprintf(os.Stderr, "Error: data-shards must be at least 1\n")
		os.Exit(1)
	}
	if *parityShards < 1 {
		fmt.Fprintf(os.Stderr, "Error: parity-shards must be at least 1\n")
		os.Exit(1)
	}
	if *repairInterval < time.Second {
		fmt.Fprintf(os.Stderr, "Error: repair-interval must be at least 1 second\n")
		os.Exit(1)
	}

	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] mountpoint\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	mountpoint := flag.Arg(0)

	bucketURLs, err := readBucketsFile(*bucketsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading buckets file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Mounting RABIDFS at %s\n", mountpoint)
	fmt.Printf("Using manifest: %s\n", *manifestPath)
	fmt.Printf("Using %d buckets from: %s\n", len(bucketURLs), *bucketsPath)
	fmt.Printf("Reed-Solomon configuration: %d data + %d parity shards\n", *dataShards, *parityShards)
	fmt.Printf("Repair interval: %v\n", *repairInterval)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	config := fs.Config{
		DataShards:     *dataShards,
		ParityShards:   *parityShards,
		RepairInterval: *repairInterval,
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- fs.MountWithConfig(mountpoint, *manifestPath, bucketURLs, config)
	}()

	select {
	case <-sigChan:
		fmt.Println("Received signal, unmounting...")
		err := fs.Unmount(mountpoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmounting: %v\n", err)
			os.Exit(1)
		}
	case err := <-errChan:
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error mounting: %v\n", err)
			os.Exit(1)
		}
	}
}

// Read a file containing bucket URLs, one per line
func readBucketsFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open buckets file: %w", err)
	}
	defer file.Close()

	var bucketURLs []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		bucketURLs = append(bucketURLs, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read buckets file: %w", err)
	}

	return bucketURLs, nil
}
