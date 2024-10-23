package store

import (
	// "bytes"
	"crypto/sha1"
	"encoding/hex"
	"time"

	// "encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anmolrajsoni15/anmols_distributed_file_system/crypto"
)

const defaultRootFolderName = "anmolrajsoni15"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	// Root is the folder name of the root, containing all the folders/files of the system.
	Root              string
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
	chunks map[string][]byte
	mu     sync.RWMutex
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}

	return &Store{
		StoreOpts: opts,
		chunks:    make(map[string][]byte),
	}
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())

	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	// log.Printf("writing [%s] to disk", key)
	// log.Printf("writing [%s] to disk", id)
	// manifest, chunks, err := ChunkFile(r)
	// if err != nil {
	// 	fmt.Println("Error chunking file:", err)
	// 	return 0, err
	// }

	// log.Printf("writing [%s] to disk", manifest.Chunks)

	// s.mu.Lock()
	// for _, chunk := range chunks {
	// 	hashStr := hex.EncodeToString(chunk.Hash)
	// 	s.chunks[hashStr] = chunk.Data
	// }
	// s.mu.Unlock()

	// // Store manifest
	// manifestKey := fmt.Sprintf("%s.manifest", key)
	// manifestData, err := json.Marshal(manifest)
	// if err != nil {
	// 	return 0, err
	// }

	// return s.writeStream(id, manifestKey, bytes.NewReader(manifestData))
	return s.writeStream(id, key, r)
}

func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := crypto.CopyDecrypt(encKey, r, f)
	return int64(n), err
}

func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	log.Print("openFileForWriting key value is: ", key)
	pathKey := s.PathTransformFunc(key)
	log.Print("openFileForWriting pathKey value is: ", pathKey)
	pathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return nil, err
	}

	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	return os.Create(fullPathWithRoot)
}

func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}

func (s *Store) Read(id string, key string) (int64, io.Reader, error) {
	// Read manifest
	// manifestKey := fmt.Sprintf("%s.manifest", key)
	// _, r, err := s.readStream(id, manifestKey)
	// if err != nil {
	// 	return 0, nil, err
	// }

	// manifestData, err := io.ReadAll(r)
	// if err != nil {
	// 	return 0, nil, err
	// }

	// var manifest FileManifest
	// if err := json.Unmarshal(manifestData, &manifest); err != nil {
	// 	return 0, nil, err
	// }

	// getChunk := func(hash string) ([]byte, error) {
	// 	s.mu.RLock()
	// 	defer s.mu.RUnlock()
	// 	data, ok := s.chunks[hash]
	// 	if !ok {
	// 		return nil, fmt.Errorf("chunk not found: %s", hash)
	// 	}
	// 	return data, nil
	// }

	// reader, err := ReassembleFile(&manifest, getChunk)
	// return manifest.TotalSize, reader, err
	return s.readStream(id, key)
}

func (s *Store) readStream(id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, nil, err
	}

	return fi.Size(), file, nil
}

func (s *Store) DecryptAndReadStream(encKey []byte, otherId string, myId string, key string) (int64, io.Reader, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, otherId, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	_, err = s.WriteDecrypt(encKey, myId, key, file)
	if err != nil {
		return 0, nil, err
	}

	time.Sleep(time.Millisecond * 500)

	return s.readStream(myId, key)
}

func (s *Store) List() ([]string, error) {
	var files []string
	err := filepath.Walk(s.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relativePath, err := filepath.Rel(s.Root, path)
			if err != nil {
				return err
			}
			files = append(files, relativePath) // Store only relative paths
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return files, nil
}
