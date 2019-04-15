package main

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"strings"
)

func VisitFiles(dir, fileSuffix string,
	cb func(kind, data string) error) error {
	fileNames, err := FileNames(dir, fileSuffix)
	if err != nil {
		return err
	}

	for _, fileName := range fileNames {
		err = cb("fileStart", fileName)
		if err != nil {
			return err
		}

		fileBytes, err := ioutil.ReadFile(dir + "/" + fileName)
		if err != nil {
			return err
		}

		s := bufio.NewScanner(bytes.NewBuffer(fileBytes))
		for s.Scan() {
			err = cb("fileLine", s.Text())
			if err != nil {
				return err
			}
		}

		err = cb("fileEnd", fileName)
		if err != nil {
			return err
		}
	}

	return nil
}

// FileNames returns file names in a dir that have a given suffix.
func FileNames(dir string, fileSuffix string) (
	fileNames []string, err error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() &&
			strings.HasSuffix(fileInfo.Name(), fileSuffix) {
			fileNames = append(fileNames, fileInfo.Name())
		}
	}

	return fileNames, nil
}
