package goddfs

import (
	"fmt"
	"regexp"
)

// Regexp for unsafe characters to replace for blob naming in DDFS
var UNSAFE_RE = regexp.MustCompile("[^A-Za-z0-9_\\-@:]")

// Returns the safe name for the blob
func safeName(n string) string {
	return UNSAFE_RE.ReplaceAllString(n, "_")
}

// Returns the blob name by index
func blobName(name string, num int) string {
	return fmt.Sprintf("%s-%d", safeName(name), num)
}
