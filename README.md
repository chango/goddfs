## goddfs
goddfs is a Go API for Disco DDFS. Requires https://github.com/hydrogen18/stalecucumber.

WARNING: This is still under development and is subject to change.

### Usage
```
go get github.com/oldmantaiter/goddfs
go get github.com/hydrogen18/stalecucumber
```

```go
import "github.com/oldmantaiter/goddfs"
import "time"
import "fmt"
...
var err error
timeout := time.Second * 100
ddfs := goddfs.NewDDFSClient("localhost", "8989", timeout)

// Set tag attribute
err = ddfs.SetTagAttr("test:tag", "test", "test attribute")
if err != nil {
    fmt.Printf("Failed to set tag attr: %s\n", err)
}

// Get tag attributes
attrs := ddfs.GetTagAttrs("test:tag")
for k, v := range attrs {
    fmt.Printf("ATTR: %s\tVAL: %s\n", k, v)
}

// Get single tag attribute
fmt.Println(ddfs.GetTagAttr("test:tag", "test"))

// Delete tag attribute
err = ddfs.DelTagAttr("test:tag", "test") 
if err != nil {
    fmt.Printf("Failed to del attr: %s\n", err)
}

// Chunk to DDFS (tag, path_to_file, replicas, delayed, chunk_size)
urls, err := ddfs.Chunk("test:tag", []string{"/tmp/fileofstuff"}, 3, true, 1048576)
if err != nil {
    fmt.Printf("Failed to chunk: %s\n", err)
}

fmt.Println("Chunked to: ", urls)

// Tag chunked blobs to another tag
ddfs.Tag("test:tag2", urls, false, false)
```