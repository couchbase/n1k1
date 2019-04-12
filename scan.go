package n1k1

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func Scan(params []interface{},
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	kind := params[0].(string)

	switch kind {
	case "filePath":
		filePath := params[1].(string)
		lazyFilePath := filePath
		ScanFile(lazyFilePath, lazyYield, lazyYieldErr) // <== inline-ok.

	case "csvData":
		csvData := params[1].(string)
		lazyCsvData := csvData
		lazyReader := strings.NewReader(lazyCsvData)
		ScanReaderAsCsv(lazyReader, lazyYield, lazyYieldErr) // <== inline-ok.

	case "repeat":
		n, err := strconv.Atoi(params[1].(string))
		if err != nil {
			lazyYieldErr(err)
			return
		}

		lazyN := n
		lazyParams := params[2:]
		for lazyI := 0; lazyI < lazyN; lazyI++ {
			Scan(lazyParams, lazyYield, lazyYieldErr) // Do not inline.
		}

	default:
		lazyYieldErr(fmt.Errorf("unknown scan kind"))
	}
}

func ScanFile(lazyFilePath string,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	if !strings.HasSuffix(lazyFilePath, ".csv") {
		lazyYieldErr(fmt.Errorf("not csv, lazyFilePath: %s", lazyFilePath))
		return
	}

	lazyReader, lazyErr := os.Open(lazyFilePath)
	if lazyErr != nil {
		lazyYieldErr(lazyErr)
		return
	}

	defer lazyReader.Close()

	ScanReaderAsCsv(lazyReader, lazyYield, lazyYieldErr) // <== inline-ok.
}

func ScanReaderAsCsv(lazyReader io.Reader,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	var lazyReaderCsv = csv.NewReader(lazyReader)

	lazyReaderCsv.ReuseRecord = true

	var lazyVals LazyVals

	for LazyTrue {
		lazyRecord, lazyErr := lazyReaderCsv.Read()
		if lazyErr != nil {
			if lazyErr != io.EOF {
				lazyYieldErr(lazyErr)
			}

			return
		}

		if len(lazyRecord) > 0 {
			lazyVals = lazyVals[:0]
			for _, v := range lazyRecord {
				lazyVals = append(lazyVals, LazyVal(v))
			}

			lazyYield(lazyVals)
		}
	}
}
