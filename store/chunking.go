package store

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

const ChunkSize = 1 << 20 // 1 MB chunks

type Chunk struct {
	Hash []byte
	Size int64
	Data []byte
}

type FileManifest struct {
	Chunks    []string
	TotalSize int64
}

func ChunkFile(r io.Reader) (*FileManifest, []Chunk, error) {
	manifest := &FileManifest{}
	var chunks []Chunk

	buf := make([]byte, ChunkSize)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if n == 0 {
			break
		}

		chunk := Chunk{
			Data: make([]byte, n),
			Size: int64(n),
		}
		copy(chunk.Data, buf[:n])

		hash := sha256.Sum256(chunk.Data)
		chunk.Hash = hash[:]
		
		hashStr := hex.EncodeToString(chunk.Hash)
		manifest.Chunks = append(manifest.Chunks, hashStr)
		manifest.TotalSize += chunk.Size
		
		chunks = append(chunks, chunk)

		if err == io.EOF {
			break
		}
	}

	return manifest, chunks, nil
}

func ReassembleFile(manifest *FileManifest, getChunk func(hash string) ([]byte, error)) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		for _, hash := range manifest.Chunks {
			data, err := getChunk(hash)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(data); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()

	return pr, nil
}