# dynamocity

```go
import "github.com/edwardsmatt/dynamocity"
```

Package `dynamocity` provides helpful types for using dynamodb; however, the `OverrideEndpointResolver`
theoretically supports creating a client for any AWS service client using [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/)

The core reason `dynamocity` exits is to provide a convenient implementation of `dynamodbattribute.Marshaler` and `dynamodbattribute.Unmarshaller` which enforces fixed timestamp precision when marshalling for DynamoDB, making it safe for using `time.Time` as a DynamoDB range key in a string type.

## Background

From the standard go library [time.RFC3339Nano](https://golang.org/pkg/time/#pkg-constants) documentation

```text
The RFC3339Nano format removes trailing zeros from the seconds field and thus may not sort correctly once formatted.
```

Given the existing [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/) uses `time.RFC3339Nano`, it is not suitable to use `time.Time` as a Dynamo DB Sort Key in a string attribute type.

Designing an efficient Dynamo Single Table design which leverages a generic composite key structure can often use string attribute types; and for correct sortability in this case truncating trailing zeros would be detrimental.

As an aside please I highly recommend the [Dynamo DB Book](https://www.dynamodbbook.com/) from [Alex DeBrie](https://twitter.com/alexbdebrie) if you're interested in some top shelf DynamoDB learning resources

### Why `dynamocity?`

Well to be honest, I was working with a chap, let's call him JimmyD. We were working through designing a dynamo table schema for a few different components. Whilst wrapping his head around the schema and patterns, JimmyD refered to the Dynamo schema as a **monstrosity** and then the **Dynamo Monstricity**, and afterwards - it became affectionately known as **Dynamocity**. 

And that's it. Thanks JimmyD

## Index

* [NanoTime](#NanoTime)
* [MillisTime](#MillisTime)
* [SecondsTime](#SecondsTime)
* [OverrideEndpointResolver](#OverrideEndpointResolver)

## Types

All of the below Time types implement fixed precision when marshalled to strings, and are therefore sortable as an index type in Dynamo, or anywhere that a string representation needs to be string sortable. Also, all of the following types implement:

* `dynamodbattribute.Marshaler`
* `dynamodbattribute.Unmarshaller`
* `fmt.Stringer`
* `json.Unmarshaler`
* `json.Marshaler`

Implementing these types make them safe for JSON, string, or dynamo (un)marshalling.

### NanoTime

`NanoTime` represents a sortable strict RFC3339 Timestamp with fixed nanosecond precision. 
Example Usage:

```go
dynamocity.NanoTime(time.Date(2020, time.April, 1, 14, 0, 0, 999000000, time.UTC)),
```

### MillisTime

`MillisTime` represents a sortable strict RFC3339 Timestamp with fixed millisecond precision. 
Example Usage:

```go
dynamocity.MillisTime(time.Date(2020, time.April, 1, 14, 0, 0, 999000000, time.UTC)),
```

### SecondsTime

`SecondsTime` represents a sortable strict RFC3339 Timestamp with fixed second precision. 
Example Usage:

```go
dynamocity.SecondsTime(time.Date(2020, time.April, 1, 14, 0, 0, 999000000, time.UTC)),
```

### OverrideEndpointResolver

The `OverrideEndpointResolver` can be used to provide a simple Client factory function. For example, creating a `*dynamodb.Client` with overrides could be as follows:

```go
// Dynamo is a utility function to return a *dynamodb.Client
func Dynamo(overrides map[string]string) (*dynamodb.Client, error) {
    awsConfig, err := external.LoadDefaultAWSConfig()
    if err != nil {
        return nil, err
    }

    awsConfig.EndpointResolver = dynamocity.MakeEndpointResolver(overrides)

    client := dynamodb.New(awsConfig)

    return client, nil
}
```

However, as previously mentioned this pattern could theoretically be used for any AWS Service [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/) - For example:

```go
// Lambda is a utility function to return a *lambda.Client
func Lambda(overrides map[string]string) (*lambda.Client, error) {
    awsConfig, err := external.LoadDefaultAWSConfig()
    if err != nil {
        return nil, err
    }

    awsConfig.EndpointResolver = dynamocity.MakeEndpointResolver(overrides)

    client := lambda.New(awsConfig)

    return client, nil
}
```

## Prerequisites

* `docker-compose`
* `go 1.12` (in alignment with the [AWS Go SDK V2](https://github.com/aws/aws-sdk-go-v2/))

## Getting Started

Execute the following to provide an explanation of tasks that are commonly used for development.

```text
make help
```  

The output explains the common make targets and what they do:

```text
Perform common development tasks
Usage: make [TARGET]
Targets:
    clean     Clean removes the vendor directory, go.mod, and go.sum files
    prepare   Sets up a go.mod, go.sum and downloads all vendor dependencies
    test      Starts a dynamo local dynamo container and runs unit and integration tests
```
