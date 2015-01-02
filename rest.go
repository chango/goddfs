package goddfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

// Wrapper for communication with disco
func (ddfs *DDFSClient) Communicate(method string, url string, data []byte) ([]byte, error) {
	var err error
	var req *http.Request

	if data != nil {
		b := bytes.NewBuffer(data)
		req, _ = http.NewRequest(method, url, b)
	} else {
		req, _ = http.NewRequest(method, url, nil)
	}

	response, err := ddfs.client.Do(req)
	if err != nil {
		errStr := fmt.Sprintf("Failed to issue %s request for %s: %s", method, url, err)
		return nil, errors.New(errStr)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		errStr := fmt.Sprintf("Failed to read response body: %s", err)
		return nil, errors.New(errStr)
	}
	if response.StatusCode >= 400 {
		errStr := fmt.Sprintf("DDFS Error [%d]: %s", response.StatusCode, string(body))
		return nil, errors.New(errStr)
	}
	return body, nil
}

// Query disco for the locations to upload blobs to
func GetUploadTargets(ddfs *DDFSClient, blob string) (*[]string, error) {
	uploadUrls := new([]string)
	url := fmt.Sprintf("http://%s/ddfs/new_blob/%s", ddfs.Url, blob)
	d, err := ddfs.Communicate("GET", url, nil)
	if err != nil {
		errStr := fmt.Sprintf("Failed to get new blob locations: %s", err)
		return nil, errors.New(errStr)
	}
	err = json.Unmarshal(d, uploadUrls)
	if err != nil {
		errStr := fmt.Sprintf("Failed to unmarshal DDFS response: %s", err)
		return nil, errors.New(errStr)
	}
	return uploadUrls, nil
}

// Upload (push) the blobs to DDFS
func Upload(ddfs *DDFSClient, blob string, data []byte) ([]string, error) {
	var urls []string
	uploadUrls, err := GetUploadTargets(ddfs, blob)
	if err != nil {
		errStr := fmt.Sprintf("Push failed: %s", err)
		return nil, errors.New(errStr)
	}
	for _, u := range *uploadUrls {
		loc, err := ddfs.Communicate("PUT", u, data)
		if err != nil {
			log.Printf("Failed to upload to %s: %s", u, err)
			continue
		}
		l, _ := strconv.Unquote(string(loc))
		urls = append(urls, l)
	}
	return urls, nil
}
