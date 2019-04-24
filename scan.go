package n1k1

import (
	"bufio" // <== genCompiler:hide
	"bytes" // <== genCompiler:hide
	"fmt"
	"io"
	"os"      // <== genCompiler:hide
	"strings" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

func Scan(params []interface{}, fields base.Fields,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {
	kind := params[0].(string)

	var lzFilePath string // !lz
	_ = lzFilePath        // !lz

	var lzReader io.Reader // !lz
	_ = lzReader           // !lz

	switch kind {
	case "filePath":
		paramsFilePath := params[1].(string)
		lzFilePath := paramsFilePath

		ScanFile(lzFilePath, fields, lzYieldVals, lzYieldErr) // !lz

	case "csvData":
		paramsCsvData := params[1].(string)
		lzCsvData := paramsCsvData
		lzReader := strings.NewReader(lzCsvData)

		ScanReaderAsCsv(lzReader, fields, lzYieldVals, lzYieldErr) // !lz

	default:
		errMsg := "unknown scan kind" // TODO: Weak string/double-quote handling.
		lzYieldErr(fmt.Errorf(errMsg))
	}
}

func ScanFile(lzFilePath string, fields base.Fields,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {
	errMsg := "file not csv" // TODO: Weak string/double-quote handling.

	fileSuffixCsv := ".csv"
	if !strings.HasSuffix(lzFilePath, fileSuffixCsv) {
		lzYieldErr(fmt.Errorf(errMsg))
		return
	}

	if LzScope {
		var lzReader io.ReadWriteCloser // !lz
		_ = lzReader                    // !lz

		lzReader, lzErr := os.Open(lzFilePath)
		if lzErr != nil {
			lzYieldErr(lzErr)
			return
		}

		defer lzReader.Close()

		ScanReaderAsCsv(lzReader, fields, lzYieldVals, lzYieldErr) // !lz
	}
}

func ScanReaderAsCsv(lzReader io.Reader, fields base.Fields,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {
	var lzValsScan base.Vals

	lzScanner := bufio.NewScanner(lzReader)
	for lzScanner.Scan() {
		lzValsScan = lzValsScan[:0]

		lzLine := lzScanner.Bytes()
		for len(lzLine) > 0 {
			lzCommaAt := bytes.IndexByte(lzLine, ',')
			if lzCommaAt < 0 {
				lzValsScan = append(lzValsScan, base.Val(lzLine))
				break
			}

			lzPart := lzLine[:lzCommaAt]
			lzValsScan = append(lzValsScan, base.Val(lzPart))
			lzLine = lzLine[lzCommaAt+1:]
		}

		if len(lzValsScan) > 0 {
			lzYieldVals(lzValsScan)
		}
	}

	if lzScanner.Err() != LzErrNil {
		lzYieldErr(lzScanner.Err())
	}
}
