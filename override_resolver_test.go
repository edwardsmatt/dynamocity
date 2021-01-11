package dynamocity_test

import (
	"testing"

	"github.com/edwardsmatt/dynamocity"
)

func Test_OverrideEndpointResolver(t *testing.T) {
	cases := []struct {
		description         string
		region              string
		endpointID          string
		overrides           map[string]string
		expectedEndpointURL string
	}{
		{
			description: "Given an overridden DynamoDB endpoint, when using ap-southeast-2 region, then return the expected endpoint",
			region:      "ap-southeast-2",
			endpointID:  "dynamodb",
			overrides: map[string]string{
				"dynamodb": "http://localhost:8000",
			},
			expectedEndpointURL: "http://localhost:8000",
		},
		{
			description:         "Given no overridden DynamoDB endpoint, when using ap-southeast-2 region, then return the endpoint for the the region",
			region:              "ap-southeast-2",
			endpointID:          "dynamodb",
			overrides:           make(map[string]string),
			expectedEndpointURL: "https://dynamodb.ap-southeast-2.amazonaws.com",
		},
		{
			description:         "Given no overridden DynamoDB endpoint, when using us-east-1 region, then return the endpoint for the the region",
			region:              "us-east-1",
			endpointID:          "dynamodb",
			overrides:           make(map[string]string),
			expectedEndpointURL: "https://dynamodb.us-east-1.amazonaws.com",
		},
	}

	for _, tc := range cases {
		r := dynamocity.MakeEndpointResolver(tc.overrides)
		endpoint, err := r.ResolveEndpoint(tc.endpointID, tc.region)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}

		if endpoint.URL != tc.expectedEndpointURL {
			t.Errorf("Unexpected endpoint. Expected '%s', Got '%s'", tc.expectedEndpointURL, endpoint.URL)
		}
	}
}
