package ObjStorerLocalFS

import (
	"io/ioutil"
	"errors"
	"fmt"
	"os"

	"github.com/HolmesProcessing/Holmes-Storage/objStorerGeneric"
)

type ObjStorerLocalFS struct {
	StorageLocation string
}

func (s ObjStorerLocalFS) Initialize(c []*objStorerGeneric.ObjDBConnector) (objStorerGeneric.ObjStorer, error) {
	s.StorageLocation = "./objstorage-local-fs"
	return s, nil
}

func (s ObjStorerLocalFS) Setup() error {
	err := os.Mkdir(s.StorageLocation, 0755)
	if err != nil {
		return err
	}
	_, err = os.Stat(s.StorageLocation)
	return err
}

func (s ObjStorerLocalFS) StoreSample(sample *objStorerGeneric.Sample) error {
	filepath := fmt.Sprintf("%s/%s",s.StorageLocation,sample.SHA256)
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return ioutil.WriteFile(filepath, sample.Data, 0644)
	} else if os.IsPermission(err) {
		return errors.New("permission denied")
	} else if os.IsExist(err) {
		return errors.New("duplicate")
	} else {
		return err
	}
}

func (s ObjStorerLocalFS) GetSample(id string) (*objStorerGeneric.Sample, error) {
	sample := &objStorerGeneric.Sample{SHA256: id}
	filepath := fmt.Sprintf("%s/%s",s.StorageLocation, sample.SHA256)
	data, err := ioutil.ReadFile(filepath)
	sample.Data = data
	return sample, err
}

// TODO: Support MultipleObjects retrieval and getting. Useful when using something over 100megs
