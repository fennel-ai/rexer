package instancemetadata

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

const MetadataV2Endpoint = "http://169.254.169.254/latest"

func GetAvailabilityZoneId() (string, error) {
	req, err := http.NewRequest("GET",  fmt.Sprintf("%s/meta-data/placement/availability-zone-id", MetadataV2Endpoint), nil)
	if err != nil {
		return "", err
	}
	token, err := instanceMetadataToken()
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Aws-Ec2-Metadata-Token", token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func instanceMetadataToken() (string, error) {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/api/token", MetadataV2Endpoint), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}