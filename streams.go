package goddfs

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"github.com/hydrogen18/stalecucumber"
	"hash/crc32"
	"log"
)

// We are going to compress EVERYTHING

type DiscoOutputStream struct {
	Hunk          *bytes.Buffer
	Output        *bytes.Buffer
	Stream        *bufio.Scanner
	PickleBuf     *bytes.Buffer
	BinaryBuf     *bytes.Buffer
	CompressBuf   *bytes.Buffer
	HunkSize      int
	MaxRecordSize int
	MinHunkSize   int
}

// Returns a new output stream, used for chunking data
func NewOutputStream(stream *bufio.Scanner) *DiscoOutputStream {
	return &DiscoOutputStream{
		HunkSize:      0,
		MaxRecordSize: MEGABYTE,
		MinHunkSize:   MEGABYTE,
		Stream:        stream,
		Output:        new(bytes.Buffer),
		Hunk:          new(bytes.Buffer),
		PickleBuf:     new(bytes.Buffer),
		BinaryBuf:     new(bytes.Buffer),
		CompressBuf:   new(bytes.Buffer),
	}
}

// Append a line to the output stream
func (output *DiscoOutputStream) Append(rec string) {
	s := len(rec)
	if s > output.MaxRecordSize {
		log.Println("Record too big to write to hunk")
		return
	}

	_, err := stalecucumber.NewPickler(output.PickleBuf).Pickle(rec)

	if err != nil {
		log.Println("Pickling error: ", err)
	}
	_, err = output.Hunk.Write(output.PickleBuf.Bytes())
	if err != nil {
		log.Println("Failed to append: ", err)
	}
	output.HunkSize += s
	defer output.PickleBuf.Reset()
	// Check if we need to flush the hunk
	if output.HunkSize > output.MinHunkSize {
		output.Flush()
	}
}

// Assemble the hunk(s) to be a chunk
func (output *DiscoOutputStream) Flush() {

	// Checksum the original values
	c := crc32.NewIEEE()
	c.Write(output.Hunk.Bytes())
	crc := c.Sum32()

	// Compress this shit
	w, _ := zlib.NewWriterLevel(output.CompressBuf, 2)
	w.Write(output.Hunk.Bytes())
	w.Close()

	var h = []interface{}{
		uint8(128 + 1),
		uint8(1),
		uint32(crc),
		uint32(output.CompressBuf.Len()),
	}

	for _, v := range h {
		err := binary.Write(output.BinaryBuf, binary.LittleEndian, v)
		if err != nil {
			log.Println("Binary write failed: ", err)
		}
	}

	// Padding for python struct
	if output.BinaryBuf.Len() < 14 {
		diff := 14 - output.BinaryBuf.Len()
		for i := diff; i > 0; i-- {
			err := binary.Write(output.BinaryBuf, binary.LittleEndian, uint8(0))
			if err != nil {
				log.Println("Binary padding failed: ", err)
			}
		}
	}
	output.Output.Write(output.BinaryBuf.Bytes())
	output.Output.Write(output.CompressBuf.Bytes())

	// Cleanup
	defer output.CompressBuf.Reset()
	defer output.BinaryBuf.Reset()
	defer output.Hunk.Reset()
	output.HunkSize = 0
}

// Get the size of the output stream buffer
func (output *DiscoOutputStream) Size() int {
	return output.Output.Len()
}
