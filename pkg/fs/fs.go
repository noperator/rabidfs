package fs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/noperator/rabidfs/pkg/crypto"
	"github.com/noperator/rabidfs/pkg/manifest"
	"github.com/noperator/rabidfs/pkg/repair"
	"github.com/noperator/rabidfs/pkg/storage"
)

type Config struct {
	DataShards     int
	ParityShards   int
	RepairInterval time.Duration
}

// FUSE filesystem
type FS struct {
	manifest *manifest.Manifest
	storage  *storage.Manager
	mutex    sync.RWMutex
	repair   *repair.Worker
	conn     *fuse.Conn
	root     fs.Node
	config   Config
}

func NewFSWithConfig(manifestPath string, bucketURLs []string, config Config) (*FS, error) {
	m, err := manifest.NewManifest(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest: %w", err)
	}

	s, err := storage.NewManager(bucketURLs)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}

	filesystem := &FS{
		manifest: m,
		storage:  s,
		config:   config,
	}

	w := repair.NewWorker(m, s, config.RepairInterval)
	w.Start()
	filesystem.repair = w

	return filesystem, nil
}

func (f *FS) getConfig() Config {
	return f.config
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{fs: f, name: ""}, nil
}

type Dir struct {
	fs   *FS
	name string
}

// Fill attr with standard metadata for directory
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Printf("Dir.Attr called for directory %s\n", d.name)
	a.Mode = os.ModeDir | 0755
	a.Mtime = time.Now()
	fmt.Printf("Dir.Attr returning mode %v, mtime %v\n", a.Mode, a.Mtime)
	return nil
}

// Look up specific entry in directory
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	fmt.Printf("Lookup called for file %s in directory %s\n", name, d.name)
	d.fs.mutex.RLock()
	defer d.fs.mutex.RUnlock()

	file, ok := d.fs.manifest.GetFile(name)
	if !ok {
		fmt.Printf("File %s not found in manifest\n", name)
		return nil, syscall.ENOENT
	}
	fmt.Printf("File %s found in manifest with size %d\n", name, file.Size)

	return &File{
		fs:   d.fs,
		name: name,
		info: file,
	}, nil
}

// Return all entries in directory
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Printf("ReadDirAll called for directory %s\n", d.name)
	d.fs.mutex.RLock()
	defer d.fs.mutex.RUnlock()

	var entries []fuse.Dirent
	files := d.fs.manifest.ListFiles()
	fmt.Printf("Found %d files in manifest\n", len(files))

	for _, file := range files {
		fmt.Printf("Adding file %s to directory listing\n", file.Name)
		entries = append(entries, fuse.Dirent{
			Name: file.Name,
			Type: fuse.DT_File,
		})
	}

	fmt.Printf("Returning %d entries\n", len(entries))
	return entries, nil
}

// NodeRemover
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	// Only support files for now
	if req.Dir {
		return syscall.EPERM
	}

	fmt.Printf("Remove called for file %s\n", req.Name)

	d.fs.mutex.Lock()
	defer d.fs.mutex.Unlock()

	fi, ok := d.fs.manifest.GetFile(req.Name)
	if !ok {
		return syscall.ENOENT
	}

	for _, c := range fi.Chunks {
		if err := d.fs.storage.DeleteChunk(c.UUID, c.BucketURL); err != nil {
			fmt.Printf("Warning: could not delete chunk %s: %v\n", c.UUID, err)
		}
	}

	if err := d.fs.manifest.RemoveFile(req.Name); err != nil {
		fmt.Printf("Failed to update manifest: %v\n", err)
		return fuse.Errno(syscall.EIO)
	}

	return nil
}

// NodeRenamer
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	fmt.Printf("Rename called for file %s to %s\n", req.OldName, req.NewName)
	d.fs.mutex.Lock()
	defer d.fs.mutex.Unlock()

	// Only support same-directory renames for now
	if d != newDir.(*Dir) {
		return fuse.Errno(syscall.EXDEV)
	}

	oldFi, ok := d.fs.manifest.GetFile(req.OldName)
	if !ok {
		return syscall.ENOENT
	}

	if _, exists := d.fs.manifest.GetFile(req.NewName); exists {
		return fuse.Errno(syscall.EEXIST)
	}

	delete(d.fs.manifest.Files, req.OldName)
	oldFi.Name = req.NewName
	d.fs.manifest.Files[req.NewName] = oldFi
	return d.fs.manifest.Save()
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fmt.Printf("Create called for file %s with mode %v\n", req.Name, req.Mode)
	d.fs.mutex.Lock()
	defer d.fs.mutex.Unlock()

	config := d.fs.getConfig()

	file := manifest.FileInfo{
		Name:         req.Name,
		Size:         0,
		Mode:         uint32(req.Mode),
		ModTime:      time.Now(),
		Chunks:       []manifest.ChunkInfo{},
		DataShards:   config.DataShards,
		ParityShards: config.ParityShards,
		ChunkSize:    128 * 1024,
	}

	fmt.Printf("Creating file %s with mode %v\n", file.Name, file.Mode)

	nonce, err := crypto.GenerateNonce()
	if err != nil {
		fmt.Printf("Failed to generate nonce: %v\n", err)
		return nil, nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	file.EncryptionNonce = nonce

	err = d.fs.manifest.AddFile(file)
	if err != nil {
		fmt.Printf("Failed to add file to manifest: %v\n", err)
		return nil, nil, fmt.Errorf("failed to add file to manifest: %w", err)
	}
	fmt.Printf("Added file %s to manifest\n", file.Name)

	node := &File{
		fs:   d.fs,
		name: req.Name,
		info: file,
	}

	fmt.Printf("Created file node for %s\n", req.Name)
	return node, node, nil
}

type File struct {
	fs         *FS
	name       string
	info       manifest.FileInfo
	writer     *stripeWriter
	appendMode bool
}

// Make the kernel forget attrs+data cached for this entry
func (f *File) invalidateKernelCache() {
	if f.fs.conn == nil {
		return
	}

	// We need to use the parent directory's NodeID and the file name
	// For simplicity, we'll use 1 as the parent NodeID (root directory)
	if err := f.fs.conn.InvalidateEntry(1, f.name); err != nil {
		fmt.Printf("InvalidateEntry(%s): %v\n", f.name, err)
	}
}

// Notice if bazil's interfaces change
var _ fs.NodeSetattrer = (*File)(nil)

// Fill attr with the standard metadata for file
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Printf("File.Attr called for file %s\n", f.name)
	f.fs.mutex.RLock()
	defer f.fs.mutex.RUnlock()

	file, ok := f.fs.manifest.GetFile(f.name)
	if !ok {
		fmt.Printf("File %s not found in manifest\n", f.name)
		return syscall.ENOENT
	}

	a.Mode = os.FileMode(file.Mode)
	a.Size = uint64(file.Size)
	a.Mtime = file.ModTime
	fmt.Printf("File.Attr returning mode %v, size %d, mtime %v\n", a.Mode, a.Size, a.Mtime)
	return nil
}

// Read data from file
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Printf("Read called for file %s, offset %d, size %d\n", f.name, req.Offset, req.Size)
	f.fs.mutex.RLock()
	defer f.fs.mutex.RUnlock()

	file, ok := f.fs.manifest.GetFile(f.name)
	if !ok {
		fmt.Printf("File %s not found in manifest\n", f.name)
		return syscall.ENOENT
	}
	fmt.Printf("File %s found in manifest with size %d and %d chunks\n", f.name, file.Size, len(file.Chunks))

	if len(file.Chunks) == 0 {
		fmt.Printf("File %s has no chunks, returning empty data\n", f.name)
		return nil
	}

	// Use the efficient readAt function to read only the requested portion of the file
	data, err := readAt(file, req.Offset, int64(req.Size), f.fs.storage, f.fs.manifest.GetEncryptionKey())
	if err != nil {
		fmt.Printf("Failed to read file: %v\n", err)
		return fmt.Errorf("failed to read file: %w", err)
	}

	resp.Data = data
	fmt.Printf("Returning %d bytes from offset %d\n", len(resp.Data), req.Offset)
	return nil
}

// Write data to ta file
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fmt.Printf("Write called for file %s, offset %d, size %d\n", f.name, req.Offset, len(req.Data))
	f.fs.mutex.Lock()
	defer f.fs.mutex.Unlock()

	fi, ok := f.fs.manifest.GetFile(f.name)
	if !ok {
		fmt.Printf("File %s not found in manifest\n", f.name)
		return syscall.ENOENT
	}

	if f.appendMode {
		fmt.Printf("Append mode: adjusting offset from %d to %d (file size)\n", req.Offset, fi.Size)
		req.Offset = fi.Size
	}

	if req.Offset != fi.Size {
		fmt.Printf("Random writes are not supported, got offset %d, expected %d\n", req.Offset, fi.Size)
		return fmt.Errorf("random writes are not supported")
	}

	if f.writer == nil {
		f.writer = newStripeWriter(fi, f.fs.storage, f.fs.manifest.GetEncryptionKey(), f.fs.manifest)
	}

	n, err := f.writer.Write(req.Data)
	if err != nil {
		fmt.Printf("Failed to write data: %v\n", err)
		return fmt.Errorf("failed to write data: %w", err)
	}

	resp.Size = n
	fmt.Printf("Write operation completed successfully, wrote %d bytes\n", resp.Size)

	return nil
}

// Flush any pending writes to file
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fmt.Printf("Fsync called for file %s\n", f.name)

	f.fs.mutex.Lock()
	defer f.fs.mutex.Unlock()

	if f.writer != nil {
		fmt.Printf("Flushing partial stripe now to ensure data durability\n")
		if err := f.writer.Close(); err != nil {
			fmt.Printf("Failed to flush writer: %v\n", err)
			return fmt.Errorf("failed to flush writer: %w", err)
		}

		f.writer = nil

		// Invalidate the kernel's cache right after writing the tail stripe
		// This forces the kernel to ask for fresh attributes next time
		f.invalidateKernelCache()
	}
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fmt.Printf("Open called for %s with flags %v\n", f.name, req.Flags)

	f.fs.mutex.Lock()
	defer f.fs.mutex.Unlock()

	fi, ok := f.fs.manifest.GetFile(f.name)
	if !ok {
		return nil, syscall.ENOENT
	}

	f.appendMode = req.Flags&fuse.OpenAppend != 0
	if f.appendMode {
		fmt.Printf("File %s opened with O_APPEND flag\n", f.name)
	}

	if req.Flags&fuse.OpenTruncate != 0 {
		fmt.Printf("Truncating %s to 0 bytes\n", f.name)

		for _, c := range fi.Chunks {
			_ = f.fs.storage.DeleteChunk(c.UUID, c.BucketURL)
		}

		fi.Chunks = nil
		fi.Size = 0
		fi.ModTime = time.Now()

		if err := f.fs.manifest.UpdateFile(fi); err != nil {
			return nil, err
		}

		f.info = fi
		f.writer = nil
	}

	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if f.writer != nil {
		fmt.Printf("Release called for file %s, flushing final partial stripe\n", f.name)
		_ = f.writer.Close()
		f.writer = nil

		// Invalidate the kernel's cache right after writing the tail stripe
		// This forces the kernel to ask for fresh attributes next time
		f.invalidateKernelCache()
	}
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fmt.Printf("Flush called for file %s\n", f.name)

	return nil
}

// Handle chmod/chown/truncate
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if !req.Valid.Size() {
		return nil
	}

	fmt.Printf("Setattr called for file %s, setting size to %d\n", f.name, req.Size)

	f.fs.mutex.Lock()
	defer f.fs.mutex.Unlock()

	fi, ok := f.fs.manifest.GetFile(f.name)
	if !ok {
		return syscall.ENOENT
	}

	// For now, only handle truncations
	if req.Size > uint64(fi.Size) {
		return fuse.Errno(syscall.EOPNOTSUPP)
	}

	if req.Size == uint64(fi.Size) {
		return nil
	}

	if req.Size != 0 {
		return fuse.Errno(syscall.EOPNOTSUPP)
	}

	fmt.Printf("Truncating %s to 0 bytes via Setattr\n", f.name)
	for _, c := range fi.Chunks {
		_ = f.fs.storage.DeleteChunk(c.UUID, c.BucketURL)
	}
	fi.Chunks = nil
	fi.Size = 0
	fi.ModTime = time.Now()

	if err := f.fs.manifest.UpdateFile(fi); err != nil {
		return err
	}

	f.info = fi
	f.writer = nil
	resp.Attr.Size = 0
	return nil
}

var currentFS *FS
var fsMutex sync.Mutex

func MountWithConfig(mountpoint string, manifestPath string, bucketURLs []string, config Config) error {
	filesystem, err := NewFSWithConfig(manifestPath, bucketURLs, config)
	if err != nil {
		return fmt.Errorf("failed to create filesystem: %w", err)
	}

	fsMutex.Lock()
	currentFS = filesystem
	fsMutex.Unlock()

	mountOpts := []fuse.MountOption{
		fuse.FSName("rabidfs"),
		fuse.Subtype("rabidfs"),
	}

	if os.Geteuid() == 0 {
		mountOpts = append(mountOpts, fuse.AllowOther())
	} else {
		fuseConf, err := os.ReadFile("/etc/fuse.conf")
		if err == nil && strings.Contains(string(fuseConf), "user_allow_other") && !strings.Contains(string(fuseConf), "#user_allow_other") {
			mountOpts = append(mountOpts, fuse.AllowOther())
		} else {
			fmt.Println("Warning: AllowOther option not used because user_allow_other is not set in /etc/fuse.conf")
		}
	}

	c, err := fuse.Mount(
		mountpoint,
		mountOpts...,
	)
	if err != nil {
		return fmt.Errorf("failed to mount filesystem: %w", err)
	}
	defer c.Close()

	filesystem.conn = c

	root, err := filesystem.Root()
	if err != nil {
		return fmt.Errorf("failed to get root node: %w", err)
	}
	filesystem.root = root

	err = fs.Serve(c, filesystem)
	if err != nil {
		return fmt.Errorf("failed to serve filesystem: %w", err)
	}

	return nil
}

func Unmount(mountpoint string) error {
	fsMutex.Lock()
	if currentFS != nil && currentFS.repair != nil {
		fmt.Println("Stopping repair worker...")
		currentFS.repair.Stop()
	}
	currentFS = nil
	fsMutex.Unlock()

	return fuse.Unmount(mountpoint)
}
