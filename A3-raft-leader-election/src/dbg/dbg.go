package dbg

import (
	"fmt"
	"log"
)

// Log prints a message to the log, along with its tags.
func Log(msg string, tags []string) {
	log.Printf("msg=\"%v\" tags=\"%v\"", msg, formatTags(tags))
}

// Logf prints a formatted message to the log, along with its tags.
func Logf(format string, tags []string, a ...interface{}) {
	log.Printf("msg=\"%v\" tags=\"%v\"", fmt.Sprintf(format, a...), formatTags(tags))
}

// LogKVs prints a message to the log, along with its tags, and followed by a list
// of key-value pairs specified by kvs.
func LogKVs(msg string, tags []string, kvs map[string]interface{}) {
	log.Printf("msg=\"%v\" tags=\"%v\" %v", msg, formatTags(tags), formatKVs(kvs))
}

// formatTags formats the tags array into a string comma-separated list.
func formatTags(tags []string) string {
	if tags == nil {
		return ""
	}

	s := ""
	for i, tag := range tags {
		if i < len(tags)-1 {
			s += tag + ","
		} else {
			s += tag // exclude tailing comma
		}
	}
	return s
}

// formatKVs formats the kvs map as a string list of "key=value" pieces.
func formatKVs(kvs map[string]interface{}) string {
	if kvs == nil {
		return ""
	}

	s := ""
	i := 0
	for k, v := range kvs {
		if i < len(kvs)-1 {
			s += fmt.Sprintf("%v=%v ", k, v)
		} else {
			s += fmt.Sprintf("%v=%v", k, v) // exclude trailing space
		}
		i++
	}
	return s
}
