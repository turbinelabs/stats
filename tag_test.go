package stats

import (
	"testing"

	"github.com/turbinelabs/test/assert"
)

func TestNewTag(t *testing.T) {
	tag := NewTag("xyz")
	assert.Equal(t, tag, Tag{K: "xyz"})
}

func TestNewKVTag(t *testing.T) {
	tag := NewKVTag("xyz", "pdq")
	assert.Equal(t, tag, Tag{K: "xyz", V: "pdq"})
}
