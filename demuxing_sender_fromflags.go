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

	tbnflag "github.com/turbinelabs/nonstdlib/flag"
)

const (
	transformTagsDesc = `
Defines one or more transformations for tags. A tag with a specific name whose value matches
a regular expression can be transformed into one or more tags with values extracted from
subexpressions of the regular expression. Transformations are specified as follows:

    tag=/regex/,n1,n2...

where tag is the name of the tag to be transformed, regex is a regular expression with 1 or
more subexpressions, and n1,n2... is a sequence of names for the tags formed from the regular
expression's subexpressions (matching groups). Any character may be used in place of the
slashes (/) to delimit the regular expression. There must be at least one subexpression in the
regular expression. There must be exactly as many names as subexpressions. If one of the names
is the original tag name, the original tag is replaced with the transformed value. Otherwise,
the original tag is passed through unchanged. Multiple transformations may be separated by
semicolons (;). Any character may be escaped with a backslash (\).

Examples:
    foo=/^(.+):.*x=([0-9]+)/,foo,bar
    foo=@.*y=([A-Za-z_]+)@,yval
`
)

type demuxingSenderFromFlags struct {
	config string
}

func newDemuxingSenderFromFlags(fs tbnflag.FlagSet) *demuxingSenderFromFlags {
	ff := &demuxingSenderFromFlags{}

	fs.StringVar(
		&ff.config,
		"transform-tags",
		"",
		transformTagsDesc,
	)

	return ff
}

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

// parseConfig parses the value of the configuration flag into 0 or more demuxTag
// instances.
func (ff *demuxingSenderFromFlags) parseConfig() ([]demuxTag, error) {
	if ff.config == "" {
		return nil, nil
	}

	demuxTags := []demuxTag{}

	reader := strings.NewReader(ff.config)
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

		dt, err := newDemuxTag(tag, regex, names)
		if err != nil {
			return nil, fmt.Errorf("char %d-%d: %s", startPos, pos, err.Error())
		}
		demuxTags = append(demuxTags, dt)
	}

	return demuxTags, nil
}

func (ff *demuxingSenderFromFlags) Validate() error {
	_, err := ff.parseConfig()
	return err
}

func (ff *demuxingSenderFromFlags) Make(underlying xstatsSender, c cleaner) (xstatsSender, error) {
	configs, err := ff.parseConfig()
	if err != nil {
		return nil, err
	}

	// The configs may be empty, in which case this will just return underlying.
	return newDemuxingSender(underlying, c, configs), nil
}
