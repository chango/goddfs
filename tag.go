package goddfs

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Struct for tag information unmarshaling
type DDFSTag struct {
	Version      int               `json:"version"`
	ID           string            `json:"id"`
	LastModified string            `json:"last-modified"`
	Urls         [][]string        `json:"urls"`
	UserData     map[string]string `json:"user-data"`
}

// Struct for pushing and tagging operations
type TagConfig struct {
	Delayed string
	Update  string
}

// Get a new tag config for use with pushing
func NewTagConfig(d bool, u bool) *TagConfig {
	var delayed string = ""
	var update string = ""
	if d {
		delayed = "1"
	}
	if u {
		update = "1"
	}
	return &TagConfig{
		delayed,
		update,
	}
}

// Get all the tag information
func GetTag(ddfs *DDFSClient, tag string) (*DDFSTag, error) {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s", ddfs.Url, tag)
	data, err := ddfs.Communicate("GET", url, nil)
	if err != nil {
		errStr := fmt.Sprintf("Failed to get tag: %s", err)
		return nil, errors.New(errStr)
	}
	var dData DDFSTag
	err = json.Unmarshal(data, &dData)
	if err != nil {
		errStr := fmt.Sprintf("Failed to decode DDFS response: %s", err)
		return nil, errors.New(errStr)
	}
	return &dData, nil
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
func TagBlobs(ddfs *DDFSClient, tag string, u [][]string, conf *TagConfig) ([]byte, [][]string, error) {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s?delayed=%s&update=%s", ddfs.Url, tag, conf.Delayed, conf.Update)
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

func ListTags(ddfs *DDFSClient, prefix string) ([]string, error) {
	tagList := new([]string)
	url := fmt.Sprintf("http://%s/ddfs/tags/%s", ddfs.Url, prefix)
	d, err := ddfs.Communicate("GET", url, nil)
	if err != nil {
		errStr := fmt.Sprintf("Failed to list tags: %s", err)
		return nil, errors.New(errStr)
	}
	err = json.Unmarshal(d, tagList)
	if err != nil {
		errStr := fmt.Sprintf("Failed to unmarshal tag list: %s", err)
		return nil, errors.New(errStr)
	}
	return *tagList, nil
}

func TagDelete(ddfs *DDFSClient, tag string) error {
	url := fmt.Sprintf("http://%s/ddfs/tag/%s", ddfs.Url, tag)
	_, err := ddfs.Communicate("DELETE", url, nil)
	if err != nil {
		errStr := fmt.Sprintf("Failed to delete tag: %s", err)
		return errors.New(errStr)
	}
	return nil
}
