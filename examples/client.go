package main

import "fmt"
import "goddfs"
import "time"

func main() {
	// Set the timeout to 100 seconds
	timeout := time.Second * 100
	// Get a new DDFS client
	ddfs := goddfs.NewDDFSClient("localhost", "8989", timeout)
	// Set a tag attribute of "test" to "test attribute"
	ddfs.SetTagAttr("test:tag", "test", "test attribute")
	// Get all the tag attributes and print them
	attrs := ddfs.GetTagAttrs("test:tag")
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
	// Chunk /tmp/testfile.txt to test:tag with a chunk size of 6 MB and 3 replicas
	urls, err := ddfs.Chunk("test:tag", []string{"/tmp/testfile.txt"}, 3, true, goddfs.MEGABYTE*6)
	if err != nil {
		fmt.Println("Bad chunk: ", err)
	}
	fmt.Println("Blob URLS: ", urls)
	// Tag the blobs that went into test:tag to test:tag2
	ddfs.Tag("test:tag2", urls, false, false)
}
