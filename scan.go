package n1k1

import (
	"bufio" // <== lazy
	"bytes" // <== lazy
	"fmt"
	"io"
	"os" // <== lazy
	"strconv"
	"strings" // <== lazy
)

func Scan(params []interface{}, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	kind := params[0].(string)

	switch kind {
	case "filePath":
		paramsFilePath := params[1].(string)
		lazyFilePath := paramsFilePath
		ScanFile(lazyFilePath, fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "csvData":
		paramsCsvData := params[1].(string)
		lazyCsvData := paramsCsvData
		lazyReader := strings.NewReader(lazyCsvData)
		ScanReaderAsCsv(lazyReader, fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "repeat": // Useful for testing to yield repeated data.
		n, err := strconv.Atoi(params[1].(string))
		if err != nil {
			lazyYieldErr(err)
			return
		}

		lazyLoops := n
		lazyParams := params[2:]
		for lazyI := 0; lazyI < lazyLoops; lazyI++ {
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
	var lazyVals LazyVals

	lazyScanner := bufio.NewScanner(lazyReader)
	for lazyScanner.Scan() {
		lazyVals = lazyVals[:0]

		lazyLine := lazyScanner.Bytes()
		for len(lazyLine) > 0 {
			lazyCommaAt := bytes.IndexByte(lazyLine, ',')
			if lazyCommaAt < 0 {
				lazyVals = append(lazyVals, LazyVal(lazyLine))
				break
			}

			lazyPart := lazyLine[:lazyCommaAt]
			lazyVals = append(lazyVals, LazyVal(lazyPart))
			lazyLine = lazyLine[lazyCommaAt+1:]
		}

		if len(lazyVals) > 0 {
			lazyYield(lazyVals)
		}
	}
}
