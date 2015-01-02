/*
Go version of a Disco Distributed Filesystem (DDFS) client

Author: Tait Clarridge <tait@clarridge.ca>
*/
package goddfs

import (
	"net/http"
	// "path"
	"time"
)

const (
	MEGABYTE = 1048576
)

func Version() string {
	return "0.1"
}

type DDFSClient struct {
	Master string
	Port   string
	Url    string
	client http.Client
}

// Returns a new DDFSClient struct for interaction with DDFS
func NewDDFSClient(master string, port string, timeout time.Duration) *DDFSClient {
	ddfs := &DDFSClient{
		Master: master,
		Port:   port,
	}
	ddfs.Url = master + ":" + port
	ddfs.client = http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	return ddfs
}

// Get all the tag attributes
func (ddfs *DDFSClient) GetTagAttrs(tag string) map[string]string {
	tagData := GetTagAttrs(ddfs, tag)
	return tagData.UserData
}

// Get a single named attribute
func (ddfs *DDFSClient) GetTagAttr(tag string, attr string) string {
	tagData := GetTagAttrs(ddfs, tag)
	return tagData.UserData[attr]
}

// Set a single named attribute
func (ddfs *DDFSClient) SetTagAttr(tag string, attr string, val interface{}) error {
	return SetTagAttr(ddfs, tag, attr, val)
}

// Delete a single tag attribute
func (ddfs *DDFSClient) DelTagAttr(tag string, attr string) error {
	return DelTagAttr(ddfs, tag, attr)
}

// Chunk items to a tag, returns the urls the inputs were tagged to
func (ddfs *DDFSClient) Chunk(tag string, urls []string, replicas int, delayed bool, size int) ([][]string, error) {
	return ChunkToTag(ddfs, tag, urls, replicas, delayed, size)
}

// Tag urls of uploaded blobs to a tag
func (ddfs *DDFSClient) Tag(tag string, urls [][]string, delayed bool, update bool) error {
	var d string
	var u string
	if delayed == true {
		d = "1"
	} else {
		d = ""
	}
	if update == true {
		u = "1"
	} else {
		u = ""
	}
	_, _, err := TagBlobs(ddfs, tag, urls, d, u)
	return err
}

// Investigate multipart file upload to ddfs for each
// func (ddfs *DDFSClient) Push(tag string, urls []string, replicas int, delayed bool) [][]string {
// 	var uu [][]string
//     for _, url := range urls {

//     }
// 	_, x, _ := TagBlobs(ddfs, tag, urls, replicas, delayed)
// 	return x
// }
