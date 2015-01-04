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
	ChunkSize    int
	TagConf      *TagConfig
}

// Returns a new Chunker struct for chunking data
func NewChunker(ddfs *DDFSClient, loc string, size int, conf *TagConfig) *Chunker {
	var u [][]string
	// Open the file for reading
	f, err := os.Open(loc)
	if err != nil {
		log.Fatal("Failed to open file ", loc, " :", err)
	}
	c := &Chunker{}
	c.DDFS = ddfs
	c.Urls = u
	c.Scanner = bufio.NewScanner(f)
	c.OutputStream = NewOutputStream(c.Scanner)
	c.Location = loc
	c.Index = 0
	c.ChunkSize = size

	if conf == nil {
		c.TagConf = NewTagConfig(false, false)
	} else {
		c.TagConf = conf
	}
	return c

}

// Iterates through the file, flushing when the chunk size (cs) is reached
func (chunker *Chunker) ChunkIter(tag string) {
	for chunker.OutputStream.Stream.Scan() {
		if chunker.OutputStream.Size() > chunker.ChunkSize {
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
func ChunkToTag(ddfs *DDFSClient, tag string, urls []string, size int, conf *TagConfig) ([][]string, error) {
	var uu [][]string
	for _, url := range urls {
		// TODO: Make this concurrent

		// For each file to upload, create new Chunker
		c := NewChunker(ddfs, url, size, conf)

		// Run through the file and append urls as we upload them to DDFS
		c.ChunkIter(tag)

		// Tag all the blobs we uploaded
		_, _, err := TagBlobs(ddfs, tag, c.Urls, c.TagConf)
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
