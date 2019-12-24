package dynamocity_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/edwardsmatt/dynamocity"
)

func Test_OverrideResolver(t *testing.T) {

	overrides := make(map[string]string)
	overrides[dynamodb.EndpointsID] = "http://localhost:8000"

	r := dynamocity.MakeEndpointResolver(overrides)
	endpoint, err := r.ResolveEndpoint(dynamodb.EndpointsID, endpoints.ApSoutheast2RegionID)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if endpoint.URL != overrides[dynamodb.EndpointsID] {
		t.Errorf("Unexpected endpoint. Expected '%s', Got '%s'", overrides[dynamodb.EndpointsID], endpoint.URL)
	}
}

func Test_OverrideResolverWithNoOverride(t *testing.T) {

	r := dynamocity.MakeEndpointResolver(make(map[string]string))
	endpoint, err := r.ResolveEndpoint(dynamodb.EndpointsID, endpoints.ApSoutheast2RegionID)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if endpoint.URL != "https://dynamodb.ap-southeast-2.amazonaws.com" {
		t.Errorf("Unexpected endpoint. Expected '%s', Got '%s'", "https://dynamodb.ap-southeast-2.amazonaws.com", endpoint.URL)
	}
}
