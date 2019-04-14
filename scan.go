package n1k1

import (
	"bufio" // <== genCompiler:hide
	"bytes" // <== genCompiler:hide
	"fmt"
	"io"
	"os"      // <== genCompiler:hide
	"strings" // <== genCompiler:hide
)

var ErrScanKindUnknown = fmt.Errorf("unknown scan kind")

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

	default:
		lazyYieldErr(ErrScanKindUnknown)
	}
}

var FileSuffixCsv = ".csv"

var ErrFileNotCsv = fmt.Errorf("file not csv")

func ScanFile(lazyFilePath string, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	if !strings.HasSuffix(lazyFilePath, FileSuffixCsv) {
		lazyYieldErr(ErrFileNotCsv)
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
