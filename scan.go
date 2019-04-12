package n1k1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func Scan(params []interface{}, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	kind := params[0].(string)

	switch kind {
	case "filePath":
		filePath := params[1].(string)
		lazyFilePath := filePath
		ScanFile(lazyFilePath, fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "csvData":
		csvData := params[1].(string)
		lazyCsvData := csvData
		lazyReader := strings.NewReader(lazyCsvData)
		ScanReaderAsCsv(lazyReader, fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "repeat": // Useful for testing to yield repeated data.
		n, err := strconv.Atoi(params[1].(string))
		if err != nil {
			lazyYieldErr(err)
			return
		}

		lazyN := n
		lazyParams := params[2:]
		for lazyI := 0; lazyI < lazyN; lazyI++ {
			Scan(lazyParams, fields, lazyYield, lazyYieldErr) // Do not inline.
		}

	default:
		lazyYieldErr(fmt.Errorf("unknown scan kind"))
	}
}

func ScanFile(lazyFilePath string, fields Fields,
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

	ScanReaderAsCsv(lazyReader, fields, lazyYield, lazyYieldErr) // <== inlineOk
}

func ScanReaderAsCsv(lazyReader io.Reader, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	// NOTE: Can't use encoding/csv, due to handling of double-quotes.
	// TODO: Scanner does not reuse slices, creating garbage.
	lazyScanner := bufio.NewScanner(lazyReader)

	var lazyVals LazyVals

	for lazyScanner.Scan() {
		lazyLine := lazyScanner.Text()
		if len(lazyLine) > 0 {
			lazyRecord := strings.Split(lazyLine, ",")
			if len(lazyRecord) > 0 {
				lazyVals = StringsToLazyVals(lazyRecord, lazyVals[:0])

				lazyYield(lazyVals)
			}
		}
	}
}
