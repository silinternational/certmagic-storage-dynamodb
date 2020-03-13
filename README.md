# DynamoDB Storage adapter for CertMagic
[![Codeship Status for silinternational/certmagic-storage-dynamodb](https://app.codeship.com/projects/ce620930-4784-0138-3c3b-420bfa3912c0/status?branch=develop)](https://app.codeship.com/projects/388799)
[![Go Report Card](https://goreportcard.com/badge/github.com/silinternational/certmagic-storage-dynamodb)](https://goreportcard.com/report/github.com/silinternational/certmagic-storage-dynamodb)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/badges/quality-score.png?b=develop)](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/?branch=develop)

[CertMagic](https://github.com/caddyserver/certmagic) Is an awesome package for doing all the 
heavy lifting with Let's Encrypt for certificate provisioning and use. In order to support 
various methods of storing cetificates CertMagic has a 
[Storage Interface](https://pkg.go.dev/github.com/caddyserver/certmagic?tab=doc#Storage) 
allowing for multiple storage implementations. 

This package is an implementation of the Storage interface that uses DynamoDB for certificate
storage. We created this implementation for use in a clustered environment were our application
runs in containers behind a load-balancer with no shared filesystem.

## Authentication with AWS

This package uses AWS's Go library for interactions with DynamoDB. As such that library can 
detect your AWS credentials in multiple ways, in the following order:

1. Environment Variables
2. Shared Credentials file
3. Shared Configuration file (if SharedConfig is enabled)
4. EC2 Instance Metadata (credentials only)

For more information about authentication see https://docs.aws.amazon.com/sdk-for-go/api/aws/session/.

## Usage

```go
import (
    "github.com/caddyserver/certmagic"

    "github.com/silinternational/certmagic-storage-dynamodb"
)
// ...
certmagic.Default.Storage = &dynamodbstore.DynamoDBStorage{
    Table: "CertMagic",
}
```
 

## Example Create DynamoDB Table command
```
aws dynamodb create-table \
    --table-name CertMagic \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions AttributeName=Domain,AttributeType=S \
    --key-schema AttributeName=Domain,KeyType=HASH
```