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

var ScanYieldStatsEvery = 1024 // Yield stats after this many tuple yields.

func OpScan(params []interface{}, fields base.Fields, lzVars *base.Vars,
	lzYieldVals base.YieldVals, lzYieldStats base.YieldStats, lzYieldErr base.YieldErr) {
	kind := params[0].(string)

	var lzFilePath string  // !lz
	var lzReader io.Reader // !lz

	_, _ = lzFilePath, lzReader // !lz

	switch kind {
	case "filePath":
		paramsFilePath := params[1].(string)

		lzFilePath := paramsFilePath

		ScanFile(lzFilePath, fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	case "csvData":
		paramsCsvData := params[1].(string)

		lzCsvData := paramsCsvData

		lzReader := strings.NewReader(lzCsvData)

		ScanReaderAsCsv(lzReader, fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	default:
		errMsg := "unknown scan kind" // TODO: Weak string/double-quote handling.

		lzYieldErr(fmt.Errorf(errMsg))
	}
}

func ScanFile(lzFilePath string, fields base.Fields,
	lzYieldVals base.YieldVals, lzYieldStats base.YieldStats, lzYieldErr base.YieldErr) {
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

		ScanReaderAsCsv(lzReader, fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz
	}
}

func ScanReaderAsCsv(lzReader io.Reader, fields base.Fields,
	lzYieldVals base.YieldVals, lzYieldStats base.YieldStats, lzYieldErr base.YieldErr) {
	var lzValsScan base.Vals

	lzYielded := 0

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

		lzYielded++
		if lzYielded >= ScanYieldStatsEvery {
			lzYielded = 0

			var lzStats base.Stats // TODO.

			lzErr := lzYieldStats(&lzStats)
			if lzErr != nil { // Also used for early exit (e.g., LIMIT).
				lzYieldErr(lzErr)
				return
			}
		}
	}

	lzYieldErr(lzScanner.Err()) // Might be nil.
}
