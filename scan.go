package n1k1

import (
	"bufio" // <== genCompiler:hide
	"bytes" // <== genCompiler:hide
	"fmt"
	"io"
	"os"      // <== genCompiler:hide
	"strings" // <== genCompiler:hide
)

func Scan(params []interface{}, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	kind := params[0].(string)

	var lazyFilePath string // <== inlineOk
	_ = lazyFilePath        // <== inlineOk

	var lazyReader io.Reader // <== inlineOk
	_ = lazyReader           // <== inlineOk

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
		errMsg := "unknown scan kind" // TODO: Weak string/double-quote handling.
		lazyYieldErr(fmt.Errorf(errMsg))
	}
}

func ScanFile(lazyFilePath string, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	errMsg := "file not csv" // TODO: Weak string/double-quote handling.

	fileSuffixCsv := ".csv"
	if !strings.HasSuffix(lazyFilePath, fileSuffixCsv) {
		lazyYieldErr(fmt.Errorf(errMsg))
		return
	}

	if LazyScope {
		var lazyReader io.ReadWriteCloser // <== inlineOk
		_ = lazyReader                    // <== inlineOk

		lazyReader, lazyErr := os.Open(lazyFilePath)
		if lazyErr != nil {
			lazyYieldErr(lazyErr)
			return
		}

		defer lazyReader.Close()

		ScanReaderAsCsv(lazyReader, fields, lazyYield, lazyYieldErr) // <== inlineOk
	}
}

func ScanReaderAsCsv(lazyReader io.Reader, fields Fields,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	var lazyValsScan LazyVals

	lazyScanner := bufio.NewScanner(lazyReader)
	for lazyScanner.Scan() {
		lazyValsScan = lazyValsScan[:0]

		lazyLine := lazyScanner.Bytes()
		for len(lazyLine) > 0 {
			lazyCommaAt := bytes.IndexByte(lazyLine, ',')
			if lazyCommaAt < 0 {
				lazyValsScan = append(lazyValsScan, LazyVal(lazyLine))
				break
			}

			lazyPart := lazyLine[:lazyCommaAt]
			lazyValsScan = append(lazyValsScan, LazyVal(lazyPart))
			lazyLine = lazyLine[lazyCommaAt+1:]
		}

		if len(lazyValsScan) > 0 {
			lazyYield(lazyValsScan)
		}
	}
}
