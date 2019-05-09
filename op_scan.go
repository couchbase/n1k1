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

func OpScan(o *base.Op, lzVars *base.Vars,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr) {
	kind := o.Params[0].(string)

	reps := 1 // Optional repetition count, useful for perf testing.
	if len(o.Params) >= 3 {
		reps = o.Params[2].(int)
	}

	var lzFilePath string  // !lz
	var lzReader io.Reader // !lz

	_, _ = lzFilePath, lzReader // !lz

	switch kind {
	case "filePath":
		paramsFilePath := o.Params[1].(string)
		lzFilePath := paramsFilePath

		ScanFile(lzFilePath, o.Labels, lzVars, lzYieldVals, lzYieldErr, reps) // !lz

	case "csvData":
		paramsCsvData := o.Params[1].(string)
		lzCsvData := paramsCsvData

		lzReader := strings.NewReader(lzCsvData)

		ScanReaderAsCsv(lzReader, o.Labels, lzVars, lzYieldVals, lzYieldErr, reps) // !lz

	case "jsonsData": // Multiple JSON documents, one per line.
		paramsJsonsData := o.Params[1].(string)
		lzJsonsData := paramsJsonsData

		lzReader := strings.NewReader(lzJsonsData)

		ScanReaderAsJsons(lzReader, o.Labels, lzVars, lzYieldVals, lzYieldErr, reps) // !lz

	default:
		errMsg := "unknown scan kind" // TODO: Weak string/double-quote handling.

		lzYieldErr(fmt.Errorf(errMsg))
	}
}

// ---------------------------------------------------------------

func ScanFile(lzFilePath string, labels base.Labels, lzVars *base.Vars,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, reps int) {
	errMsg := "unknown file format" // TODO: Weak string/double-quote handling.

	fileSuffixCsv := ".csv"
	fileSuffixJsons := ".jsons"

	if LzScope {
		var lzReader io.ReadWriteCloser // !lz
		_ = lzReader                    // !lz

		lzReader, lzErr := os.Open(lzFilePath)
		if lzErr != nil {
			lzYieldErr(lzErr)
			return
		}

		defer lzReader.Close()

		if strings.HasSuffix(lzFilePath, fileSuffixCsv) {
			ScanReaderAsCsv(lzReader, labels, lzVars, lzYieldVals, lzYieldErr, reps) // !lz
			return
		}

		if strings.HasSuffix(lzFilePath, fileSuffixJsons) {
			ScanReaderAsJsons(lzReader, labels, lzVars, lzYieldVals, lzYieldErr, reps) // !lz
			return
		}

		lzYieldErr(fmt.Errorf(errMsg))
	}
}

// ---------------------------------------------------------------

func ScanReaderAsCsv(lzReader io.Reader, labels base.Labels, lzVars *base.Vars,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, reps int) {
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
			for lzI := 0; lzI < reps; lzI++ {
				lzYieldVals(lzValsScan)
			}
		}

		lzYielded++
		if lzYielded >= ScanYieldStatsEvery {
			lzYielded = 0

			if lzVars != nil && lzVars.Ctx != nil && lzVars.Ctx.YieldStats != nil {
				var lzStats base.Stats // TODO.

				lzErr := lzVars.Ctx.YieldStats(&lzStats)
				if lzErr != nil { // Also used for early exit (e.g., LIMIT).
					lzYieldErr(lzErr)
					return
				}
			}
		}
	}

	lzYieldErr(lzScanner.Err()) // Might be nil.
}

// ---------------------------------------------------------------

func ScanReaderAsJsons(lzReader io.Reader, labels base.Labels, lzVars *base.Vars,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, reps int) {
	var lzValsScan base.Vals

	lzYielded := 0

	lzScanner := bufio.NewScanner(lzReader)

	for lzScanner.Scan() {
		lzLine := lzScanner.Bytes()
		if len(lzLine) > 0 {
			lzValsScan = lzValsScan[:0]

			lzValsScan = append(lzValsScan, base.Val(lzLine))

			for lzI := 0; lzI < reps; lzI++ {
				lzYieldVals(lzValsScan)
			}
		}

		lzYielded++
		if lzYielded >= ScanYieldStatsEvery {
			lzYielded = 0

			if lzVars != nil && lzVars.Ctx != nil && lzVars.Ctx.YieldStats != nil {
				var lzStats base.Stats // TODO.

				lzErr := lzVars.Ctx.YieldStats(&lzStats)
				if lzErr != nil { // Also used for early exit (e.g., LIMIT).
					lzYieldErr(lzErr)
					return
				}
			}
		}
	}

	lzYieldErr(lzScanner.Err()) // Might be nil.
}
