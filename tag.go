package goddfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
)

type DDFSTag struct {
	Version      int               `json:"version"`
	ID           string            `json:"id"`
	LastModified string            `json:"last-modified"`
	Urls         [][]string        `json:"urls"`
	UserData     map[string]string `json:"user-data"`
}

// Get all the tag information
func GetTagAttrs(ddfs *DDFSClient, tag string) *DDFSTag {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s", ddfs.Url, tag)
	data, err := ddfs.Communicate("GET", url, nil)
	var dData DDFSTag
	err = json.Unmarshal(data, &dData)
	if err != nil {
		log.Println("Failed to decode DDFS response", err)
		return nil
	}
	return &dData
}

// Set a tag attribute
func SetTagAttr(ddfs *DDFSClient, tag string, attr string, val interface{}) error {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s/%s", ddfs.Url, tag, attr)
	// Disco currently has trouble with anything other than a string
	// But lets future proof this a bit
	data, err := json.Marshal(val)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to encode DDFS request", err))
	}
	_, err = ddfs.Communicate("PUT", url, data)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to set tag attribute: %s", err))
	}
	return nil
}

// Delete a tag attribute
func DelTagAttr(ddfs *DDFSClient, tag string, attr string) error {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s/%s", ddfs.Url, tag, attr)
	_, err := ddfs.Communicate("DELETE", url, nil)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to delete tag attribute: %s", err))
	}
	return err
}

// Tag the blobs to DDFS
func TagBlobs(ddfs *DDFSClient, tag string, u [][]string, delayed string, update string) ([]byte, [][]string, error) {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s?delayed=%s&update=%s", ddfs.Url, tag, delayed, update)
	urls, err := json.Marshal(u)
	if err != nil {
		errStr := fmt.Sprintf("Failed to marshal urls: %s", err)
		return nil, nil, errors.New(errStr)
	}
	t, err := ddfs.Communicate("POST", url, urls)
	if err != nil {
		errStr := fmt.Sprintf("Failed to update tag: %s", err)
		return nil, nil, errors.New(errStr)
	}
	return t, u, nil
}
