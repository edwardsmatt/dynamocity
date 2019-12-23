# dynamocity

Dynamocity is a helpful library for doing things with DynamoDB in Go using the [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/).

## Rationale

From the standard go library [time.RFC3339Nano](https://golang.org/pkg/time/#pkg-constants) documentation

    The RFC3339Nano format removes trailing zeros from the seconds field and thus may not sort correctly once formatted.

Subsequently, because the existing [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/) uses `time.RFC3339Nano`, it is not suitable to use `time.Time` as a Dynamo DB Sort Key in a string attribute type.

The reason why `dynamocity.Time` exists is because it provides an implementation `dynamodbattribute.Marshaler`, `dynamodbattribute.Unmarshaller` which enforces fixed nanosecond precision when marshalling for DynamoDB, making it safe for use as a DynamoDB range key.


## Prerequisites
* `docker-compose`
* `go 1.12` (in alignment with the [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/))

## Getting Started
Execute the following to provide an explanation of tasks that are commonly used for development.

```bash
make
```

The output explains the common make targets and what they do:
```
Perform common development tasks
Usage: make [TARGET]
Targets:
  clean			Clean removes the vendor directory, go.mod, and go.sum files
  prepare		Sets up a go.mod, go.sum and downloads all vendor dependencies
  test			Starts a dynamo local dynamo container and runs unit and integration tests
```

