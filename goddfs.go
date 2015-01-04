/*
Go version of a Disco Distributed Filesystem (DDFS) client

Author: Tait Clarridge <tait@clarridge.ca>
*/
package goddfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"time"
)

const (
	KILOBYTE = 1024.0
	MEGABYTE = 1024 * KILOBYTE
	GIGABYTE = 1024 * MEGABYTE
	TERABYTE = 1024 * GIGABYTE
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
func (ddfs *DDFSClient) GetTagAttrs(tag string) (map[string]string, error) {
	tagData, err := GetTag(ddfs, tag)
	return tagData.UserData, err
}

// Get a single named attribute
func (ddfs *DDFSClient) GetTagAttr(tag string, attr string) (string, error) {
	tagData, err := GetTag(ddfs, tag)
	return tagData.UserData[attr], err
}

// Set a single named attribute
func (ddfs *DDFSClient) SetTagAttr(tag string, attr string, val interface{}) error {
	return SetTagAttr(ddfs, tag, attr, val)
}

// Delete a single tag attribute
func (ddfs *DDFSClient) DelTagAttr(tag string, attr string) error {
	return DelTagAttr(ddfs, tag, attr)
}

// Get blob locations for the tag
func (ddfs *DDFSClient) GetBlobs(tag string) ([][]string, error) {
	tagData, err := GetTag(ddfs, tag)
	return tagData.Urls, err
}

// List DDFS tags, can pass in a prefix that the tags start with
func (ddfs *DDFSClient) List(prefix string) ([]string, error) {
	return ListTags(ddfs, prefix)
}

// Delete a tag from DDFS
func (ddfs *DDFSClient) Delete(tag string) error {
	return TagDelete(ddfs, tag)
}

// Check if tag exists
func (ddfs *DDFSClient) Exists(tag string) bool {
	_, err := GetTag(ddfs, tag)
	if err != nil {
		// We are getting a DDFS error here
		// This is the quick way for now, this could say the tag does not exist because of other DDFS errors
		return false
	}
	return true
}

// Chunk items to a tag, returns the urls the inputs were tagged to
func (ddfs *DDFSClient) Chunk(tag string, urls []string, size int, conf *TagConfig) ([][]string, error) {
	return ChunkToTag(ddfs, tag, urls, size, conf)
}

// Tag urls of uploaded blobs to a tag. Passing in nil for conf will create a default config
func (ddfs *DDFSClient) Tag(tag string, urls [][]string, conf *TagConfig) error {
	if conf == nil {
		conf = NewTagConfig(false, false)
	}
	_, _, err := TagBlobs(ddfs, tag, urls, conf)
	return err
}

// Push raw files as blobs to DDFS
func (ddfs *DDFSClient) Push(tag string, urls []string, conf *TagConfig) (map[string][][]string, error) {
	uu := make(map[string][][]string)
	for _, url := range urls {
		d, err := ioutil.ReadFile(url)
		if err != nil {
			errStr := fmt.Sprintf("Failed to read file for push: %s", err)
			return nil, errors.New(errStr)
		}
		u, err := Upload(ddfs, blobName(path.Base(url), 0), d)
		uu[url] = append(uu[url], u)
		if err != nil {
			return nil, err
		}
		_, _, err = TagBlobs(ddfs, tag, uu[url], conf)
		if err != nil {
			return nil, err
		}
	}
	return uu, nil
}
