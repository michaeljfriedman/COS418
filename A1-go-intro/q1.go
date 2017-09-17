package cos418_hw1_1

import (
	"fmt"
  "io/ioutil"
	"regexp"
  "sort"
  "strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

  // Read text document
  textBytes, err := ioutil.ReadFile(path)
  checkError(err)
  text := string(textBytes)

  // Lowercase text
  text = strings.ToLower(text)

  // Count frequency of each word
  countsByWord := make(map[string]int)
  words := strings.Fields(text)
  re, err := regexp.Compile("[^0-9a-zA-Z]+")  // matches all non-alphanumeric chars
  checkError(err)
  for _, word := range words {
    // Filter non-alphanumeric chars out
    filteredWord := re.ReplaceAllString(word, "")

    // Increment word count
    if len(filteredWord) >= charThreshold {
      countsByWord[filteredWord] += 1
    }
  }

  // Convert to WordCount slice, sort, and return top `numWords` words
  wordCounts := make([]WordCount, len(countsByWord))
  i := 0
  for word, count := range countsByWord {
    wordCounts[i] = WordCount{word, count}
    i++
  }
  sortWordCounts(wordCounts)

  return wordCounts[:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
