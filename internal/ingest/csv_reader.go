package ingest

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// readCSVRow reads a single CSV record from br, handling quoted fields,
// embedded commas, embedded newlines and doubled quote escapes. It returns
// io.EOF when no more records are available. The implementation follows RFC
// 4180 closely enough for payment feeds; it does not require encoding/csv so it
// can stream arbitrarily large files row by row.
func readCSVRow(br *bufio.Reader) ([]string, error) {
	var fields []string
	var field strings.Builder
	inQuotes := false
	started := false
	for {
		b, err := br.ReadByte()
		if err == io.EOF {
			if !started {
				return nil, io.EOF
			}
			// flush trailing field
			if inQuotes {
				return nil, errors.New("csv: unterminated quoted field")
			}
			fields = append(fields, field.String())
			return fields, nil
		}
		if err != nil {
			return nil, err
		}
		started = true
		switch b {
		case '"':
			if inQuotes {
				// Look ahead for an escaped quote.
				nb, err := br.Peek(1)
				if err == nil && len(nb) == 1 && nb[0] == '"' {
					_, _ = br.ReadByte()
					field.WriteByte('"')
					continue
				}
				inQuotes = false
			} else {
				if field.Len() == 0 {
					inQuotes = true
					continue
				}
				// quote in the middle of an unquoted field: literal.
				field.WriteByte('"')
			}
		case ',':
			if inQuotes {
				field.WriteByte(',')
			} else {
				fields = append(fields, field.String())
				field.Reset()
			}
		case '\n':
			if inQuotes {
				// embedded newline within quoted field
				field.WriteByte('\n')
				continue
			}
			fields = append(fields, field.String())
			// Strip a preceding \r if present in the last field.
			if len(fields) > 0 {
				fields[len(fields)-1] = strings.TrimSuffix(fields[len(fields)-1], "\r")
			}
			return fields, nil
		case '\r':
			if inQuotes {
				field.WriteByte('\r')
				continue
			}
			// Expect \n next; consume it.
			nb, err := br.Peek(1)
			if err == nil && len(nb) == 1 && nb[0] == '\n' {
				_, _ = br.ReadByte()
			}
			fields = append(fields, field.String())
			return fields, nil
		default:
			field.WriteByte(b)
		}
	}
}
