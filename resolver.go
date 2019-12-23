package dynamocity

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
)

// OverrideEndpointResolver is an endpoint resolver for providing overriden endpoints for AWS services
// Overriding the endpoints for services is helpful for testing, including running dynamodb-local
type OverrideEndpointResolver struct {
	region    string
	overrides map[string]string
}

// MakeEndpointResolver is a factory function for creating an aws.EndpointResolver
func MakeEndpointResolver(services map[string]string) aws.EndpointResolver {
	return &OverrideEndpointResolver{
		overrides: services,
	}
}

// ResolveEndpoint implements the EndpointResolver interface which
// resolves an endpoint for a service endpoint id and region.
func (o *OverrideEndpointResolver) ResolveEndpoint(service, region string) (aws.Endpoint, error) {
	serviceEndpoint := o.overrides[service]
	trimmedEndpoint := strings.TrimSpace(serviceEndpoint)
	if len(trimmedEndpoint) > 0 {
		return aws.Endpoint{
			URL: trimmedEndpoint,
		}, nil
	}
	return endpoints.NewDefaultResolver().ResolveEndpoint(service, region)
}
