package cmd

import (
	"io/ioutil"
	"strings"
)

func SourceFiles(dir string) (fileNames []string, err error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() &&
			strings.HasSuffix(fileInfo.Name(), ".go"){
			fileNames = append(fileNames, fileInfo.Name())
		}
	}

	return fileNames, nil
}
