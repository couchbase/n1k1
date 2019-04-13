package cmd

import (
	"io/ioutil"
	"strings"
)

func FileNames(dir string, suffix string) (fileNames []string, err error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() &&
			strings.HasSuffix(fileInfo.Name(), suffix) {
			fileNames = append(fileNames, fileInfo.Name())
		}
	}

	return fileNames, nil
}
