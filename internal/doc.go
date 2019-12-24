// Package internal provides utility types for supporting testing time.
//
// OverrideEndpointResolver is a utility resolver for replacing the endpoint for a dynamo
// service to use http://localhost:8000 for testing with a dynamodb-local
//
// aws_api_types.go supplies a number of utility functions specifically for scaffolding
// a test database.
package internal
