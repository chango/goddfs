package main

import "fmt"
import "github.com/chango/goddfs"
import "time"

func main() {
	// Set the timeout to 100 seconds
	timeout := time.Second * 100
	// Get a new DDFS client
	ddfs := goddfs.NewDDFSClient("localhost", "8989", timeout)
	// Set a tag attribute of "test" to "test attribute"
	ddfs.SetTagAttr("test:tag", "test", "test attribute")
	// Get all the tag attributes and print them
	attrs, _ := ddfs.GetTagAttrs("test:tag")
	for k, v := range attrs {
		fmt.Printf("ATTR: %s\tVAL: %s\n", k, v)
	}
	// Set a new tag attribute for test2
	err := ddfs.SetTagAttr("test:tag", "test2", "stuff")
	if err != nil {
		fmt.Println("BAD SET ", err)
	}
	// Delete the newly created tag attribute test2
	err = ddfs.DelTagAttr("test:tag", "test2")
	if err != nil {
		fmt.Println("BAD DEL ", err)
	}
	// New tag config with delayed true
	tconf := goddfs.NewTagConfig(true, false)
	// Chunk /tmp/testfile.txt to test:tag with a chunk size of 6 MB
	urls, err := ddfs.Chunk("test:tag", []string{"/tmp/testfile.txt"}, goddfs.MEGABYTE*6, tconf)
	if err != nil {
		fmt.Println("Bad chunk: ", err)
	}
	fmt.Println("Blob URLS: ", urls)
	// Tag the blobs that went into test:tag to test:tag2
	ddfs.Tag("test:tag2", urls, tconf)
	// Delete the tag we just created
	ddfs.Delete("test:tag2")
	// Push the files without chunking into DDFS
	uu, err := ddfs.Push("test:tag:3", []string{"/tmp/testfile2.txt"}, tconf)
	if err != nil {
		fmt.Println("Bad push: ", err)
	}
	// List all tags starting with test
	tags, _ := ddfs.List("test")
	for _, tag := range tags {
		fmt.Println("Found Tag: ", tag)
	}
}
