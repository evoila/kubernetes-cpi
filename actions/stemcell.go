package actions

import (
	"github.com/evoila/kubernetes-cpi/cpi"
)

type StemcellCloudProperties struct {
	Image string `json:"image"`
}

func CreateStemcell(image string, cloudProps StemcellCloudProperties) (cpi.StemcellCID, error) {
	return cpi.StemcellCID(cloudProps.Image), nil
}

func DeleteStemcell(stemcellCID cpi.StemcellCID) error {
	return nil
}

func Info() (map[string]string, error) {
	var m map[string]string
	m = make(map[string]string)
	m["stemcell_formats"] = "raw"

	return m, nil
}
