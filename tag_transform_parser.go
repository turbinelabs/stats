/*
Copyright 2018 Turbine Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// readRune reads a single rune from reader and increments *pos. Returns the rune, a
// EOF flag, and an error.
func readRune(reader *strings.Reader, pos *int) (rune, bool, error) {
	r, n, err := reader.ReadRune()
	if err == io.EOF {
		return utf8.RuneError, true, nil
	}

	if r == utf8.RuneError {
		return r, false, fmt.Errorf("char %d: invalid UTF-8 character in transformation", *pos)
	}

	*pos += n
	return r, false, nil
}

// readRuneNoEOF invokes readRune and returns the result, converting EOF into an error.
func readRuneNoEOF(reader *strings.Reader, pos *int) (rune, error) {
	r, eof, err := readRune(reader, pos)
	if eof {
		return r, fmt.Errorf("char %d: unexpected end of transformation", *pos)
	}
	return r, err
}

// readUntilDelim reads runes from reader, incrementing *pos until either EOF or one
// of the given delimiters is encountered. Removes backslash-escaping from all
// characters. Returns the string read, the delimiter rune, an EOF flag, and an
// error. If no runes were read before EOF or a delimiter, an error is returned. The
// returned delimiter rune is utf8.RuneError on EOF.
func readUntilDelim(reader *strings.Reader, pos *int, delims ...rune) (string, rune, bool, error) {
	result := &strings.Builder{}
	escaped := false
	for {
		r, eof, err := readRune(reader, pos)
		if eof {
			var err error
			if result.Len() == 0 {
				err = fmt.Errorf("char %d: unexpected end of transformation", *pos)
			}
			return result.String(), utf8.RuneError, true, err
		}
		if err != nil {
			return "", utf8.RuneError, false, err
		}

		if !escaped {
			for _, delim := range delims {
				if r == delim {
					var err error
					if result.Len() == 0 {
						err = fmt.Errorf(
							"char %d: expected at least 1 character before '%c'",
							*pos,
							delim,
						)
					}
					return result.String(), r, false, err
				}
			}

			if r == '\\' {
				escaped = true
				continue
			}
		}

		escaped = false
		result.WriteRune(r)
	}
}

// readUntilDelimNoEOF invokes readUntilDelim, converting EOF into an error even if a
// string was read.
func readUntilDelimNoEOF(reader *strings.Reader, pos *int, delims ...rune) (string, rune, error) {
	s, r, eof, err := readUntilDelim(reader, pos, delims...)
	if eof {
		return s, r, fmt.Errorf("char %d: unexpected end of transformation", *pos)
	}
	return s, r, err
}

// parseTagTransforms parses the value of the tag transform configuration flag into a
// tagTransformer.
func parseTagTransforms(config string) (*tagTransformer, error) {
	if config == "" {
		return newTagTransformer(nil), nil
	}

	tagTransforms := []tagTransform{}

	reader := strings.NewReader(config)
	pos := 0
	for reader.Len() > 0 {
		startPos := pos

		var (
			tag, regex string
			delim      rune
			err        error
		)

		// Read the original tag name and delimiter.
		tag, _, err = readUntilDelimNoEOF(reader, &pos, '=')
		if err != nil {
			return nil, err
		}

		// Read the regex delimiter (e.g. '/', but can be anything).
		delim, err = readRuneNoEOF(reader, &pos)
		if err != nil {
			return nil, err
		}

		// Read the regex.
		regex, _, err = readUntilDelimNoEOF(reader, &pos, delim)
		if err != nil {
			return nil, err
		}

		// Read the comma separating regex from mapped tag names.
		delim, err = readRuneNoEOF(reader, &pos)
		if err != nil {
			return nil, err
		}
		if delim != ',' {
			return nil, fmt.Errorf("char %d: expected ',' after regular expression", pos)
		}

		// Read names until we run out of data or find the ';' separator between two
		// configs.
		names := []string{}
		for reader.Len() > 0 && delim != ';' {
			var name string

			// Read the next name, terminated by EOF, ',' or ';'.
			name, delim, _, err = readUntilDelim(reader, &pos, ',', ';')
			if err != nil {
				return nil, err
			}

			names = append(names, name)
		}

		tt, err := newTagTransform(tag, regex, names)
		if err != nil {
			return nil, fmt.Errorf("char %d-%d: %s", startPos, pos, err.Error())
		}
		tagTransforms = append(tagTransforms, tt)
	}

	return newTagTransformer(tagTransforms), nil
}
