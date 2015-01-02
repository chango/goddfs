package goddfs

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
)

type Chunker struct {
	DDFS         *DDFSClient
	Urls         [][]string
	Scanner      *bufio.Scanner
	OutputStream *DiscoOutputStream
	Location     string
	Index        int
}

// Returns a new Chunker struct for chunking data
func NewChunker(ddfs *DDFSClient, loc string) *Chunker {
	var u [][]string
	// Open the file for reading
	f, err := os.Open(loc)
	if err != nil {
		log.Fatal("Failed to open file ", loc, " :", err)
	}
	scanner := bufio.NewScanner(f)
	o := NewOutputStream(scanner)
	return &Chunker{
		ddfs,
		u,
		scanner,
		o,
		loc,
		0,
	}
}

// Iterates through the file, flushing when the chunk size (cs) is reached
func (chunker *Chunker) ChunkIter(tag string, cs int) {
	for chunker.OutputStream.Stream.Scan() {
		if chunker.OutputStream.Size() > cs {
			chunker.Flush()
			chunker.Index += 1
		}
		chunker.OutputStream.Append(chunker.OutputStream.Stream.Text())
	}
	if err := chunker.OutputStream.Stream.Err(); err != nil {
		log.Fatal("FATAL: ", err)
	}
	if chunker.OutputStream.HunkSize > 0 {
		chunker.Flush()
	}
}

// Write the chunk to DDFS and reset the Output stream
func (chunker *Chunker) Flush() {
	chunker.OutputStream.Flush()
	chunker.WriteChunk(blobName(path.Base(chunker.Location), chunker.Index), chunker.OutputStream.Output.Bytes())
	// Set size and reset Output stream buffer
	chunker.OutputStream.Output.Reset()
}

// Chunk the files denoted by urls []string to a DDFS tag
func ChunkToTag(ddfs *DDFSClient, tag string, urls []string, replicas int, delayed bool, size int) ([][]string, error) {
	var uu [][]string
	for _, url := range urls {
		// TODO: Make this concurrent

		// For each file to upload, create new Chunker
		c := NewChunker(ddfs, url)

		// Run through the file and append urls as we upload them to DDFS
		c.ChunkIter(tag, size)

		// Tag all the blobs we uploaded
		_, _, err := TagBlobs(ddfs, tag, c.Urls, "", "")
		if err != nil {
			errStr := fmt.Sprintf("Chunk failed: %s", err)
			return nil, errors.New(errStr)
		}
		for _, u := range c.Urls {
			uu = append(uu, u)
		}
	}
	return uu, nil
}

// Write the Chunk to DDFS
func (chunker *Chunker) WriteChunk(blob string, data []byte) {
	urls, err := Upload(chunker.DDFS, blob, data)
	if err != nil {
		log.Printf("Chunk failed to push: %s", err)
		return
	}
	chunker.Urls = append(chunker.Urls, urls)
}
