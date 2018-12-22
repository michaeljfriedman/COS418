package dbg

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
)

// errNotStruct is the constant error used by tryParseStruct.
var errNotStruct = errors.New("interface is not a struct")

// Log prints a message to the log, along with its tags.
func Log(msg string, tags []string) {
	log.Printf("msg=\"%v\" tags=\"%v\"", msg, formatTags(tags))
}

// Logf prints a formatted message to the log, along with its tags.
func Logf(format string, tags []string, a ...interface{}) {
	log.Printf("msg=\"%v\" tags=\"%v\"", fmt.Sprintf(format, a...), formatTags(tags))
}

// LogKVs prints a message to the log, along with its tags, and followed by a
// list of key-value pairs specified by kvs. Returns an error if there was an
// error while parsing.
func LogKVs(msg string, tags []string, kvs map[string]interface{}) error {
	kvStr, err := formatKVs(kvs)
	log.Printf("msg=\"%v\" tags=\"%v\" %v", msg, formatTags(tags), kvStr)
	if err != nil {
		return err
	}
	return nil
}

// LogIf prints a message based on the conditional. Pass the empty string for
// either message to print nothing in that case.
func LogIf(conditional bool, msgIfTrue string, msgIfFalse string, tags []string) {
	if conditional && msgIfTrue != "" {
		Log(msgIfTrue, tags)
	} else if !conditional && msgIfFalse != "" {
		Log(msgIfFalse, tags)
	}
}

// LogKVsIf is analogous to LogIf, but for LogKVs.
func LogKVsIf(conditional bool, msgIfTrue string, msgIfFalse string,
	tags []string, kvs map[string]interface{}) error {
	if conditional && msgIfTrue != "" {
		return LogKVs(msgIfTrue, tags, kvs)
	} else if !conditional && msgIfFalse != "" {
		return LogKVs(msgIfFalse, tags, kvs)
	}
	return nil
}

// formatTags formats the tags array into a string comma-separated list. Returns
// the string.
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
// Returns a string that parses the values as best as possible, and possibly
// an error if something went wrong during the process.
func formatKVs(kvs map[string]interface{}) (string, error) {
	if kvs == nil {
		return "", nil
	}

	ks := make([]string, len(kvs))
	i := 0
	for k := range kvs {
		ks[i] = k
		i++
	}
	sort.Strings(ks)

	s := ""
	var retErr error
	for i, k := range ks {
		v := kvs[k]
		valStr, err := tryParseStruct(v)
		if err == errNotStruct {
			valStr = fmt.Sprintf("%v", v)
		} else if err != nil {
			retErr = err
		}

		var nextChar string
		if i < len(kvs)-1 {
			nextChar = " "
		}
		s += fmt.Sprintf("%v=%v"+nextChar, k, valStr)
		i++
	}
	return s, retErr
}

// tryParseStruct attempts to parse a struct value into a custom string.
// If i is a struct and is fully parseable (meaning all of its fields were
// exported/readable), returns the string. Otherwise, returns an error
// (this error will be errNotStruct if i was not a struct, so you can check
// for this case specifically).
func tryParseStruct(i interface{}) (string, error) {
	var s string
	v := reflect.ValueOf(i)

	// Dereference pointer if necessary
	if v.Kind() == reflect.Ptr {
		s = "&"
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return "", errNotStruct
	}

	// Parse struct
	t := v.Type()
	format := "%v:%v"
	s += "{"
	unreadableField := false
	for j := 0; j < v.NumField(); j++ {
		var nextChar string
		if j < v.NumField()-1 {
			nextChar = " "
		} else {
			nextChar = "}"
		}

		if !v.Field(j).CanInterface() {
			unreadableField = true
			continue
		}
		s += fmt.Sprintf(format+nextChar, t.Field(j).Name, v.Field(j).Interface())
	}

	var err error
	if unreadableField {
		err = errors.New("could not read all fields of struct (maybe not all exported)")
	}
	return s, err
}
