package dynamocity_test

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/edwardsmatt/dynamocity"
)

func Test_OverrideEndpointResolver(t *testing.T) {
	cases := []struct {
		description         string
		region              string
		endpointID          string
		overrides           map[string]string
		expectedEndpointURL string
		expectedErr         error
	}{
		{
			description: "Given an overridden DynamoDB endpoint, when using ap-southeast-2 region, then return the expected endpoint",
			region:      "ap-southeast-2",
			endpointID:  dynamodb.ServiceID,
			overrides: map[string]string{
				dynamodb.ServiceID: "http://localhost:8000",
			},
			expectedEndpointURL: "http://localhost:8000",
			expectedErr:         nil,
		},
		{
			description: "Given no overridden DynamoDB endpoint, then return EndpointNotFoundError sentinel error to resort to fallback",
			region:      "ap-southeast-2",
			endpointID:  dynamodb.ServiceID,
			overrides:   make(map[string]string),
			expectedErr: &aws.EndpointNotFoundError{},
		},
	}

	for _, tc := range cases {
		r := dynamocity.MakeEndpointResolver(tc.overrides)
		endpoint, err := r.ResolveEndpoint(tc.endpointID, tc.region)
		if (tc.expectedErr == nil && err != nil) || (tc.expectedErr != nil && err == nil) {
			t.Error(err)
			t.FailNow()
		}

		if tc.expectedErr == nil && endpoint.URL != tc.expectedEndpointURL {
			t.Errorf("Unexpected endpoint. Expected '%s', Got '%s'", tc.expectedEndpointURL, endpoint.URL)
		}

		if tc.expectedErr != nil && reflect.TypeOf(err) != reflect.TypeOf(tc.expectedErr) {
			t.Errorf("Unexpected endpoint. Expected '%s', Got '%s'", tc.expectedEndpointURL, endpoint.URL)
		}
	}
}
