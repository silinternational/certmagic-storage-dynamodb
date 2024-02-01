# DynamoDB Storage adapter for CertMagic
[![Codeship Status for silinternational/certmagic-storage-dynamodb](https://app.codeship.com/projects/ce620930-4784-0138-3c3b-420bfa3912c0/status?branch=develop)](https://app.codeship.com/projects/388799)
[![Go Report Card](https://goreportcard.com/badge/github.com/silinternational/certmagic-storage-dynamodb)](https://goreportcard.com/report/github.com/silinternational/certmagic-storage-dynamodb)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/badges/quality-score.png?b=develop)](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/?branch=develop)
[![Code Coverage](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/badges/coverage.png?b=develop)](https://scrutinizer-ci.com/g/silinternational/certmagic-storage-dynamodb/?branch=develop)

[CertMagic](https://github.com/caddyserver/certmagic) Is an awesome package for doing all the 
heavy lifting with Let's Encrypt for certificate provisioning and use. In order to be flexible 
about how you use CertMagic, it has support for various methods of storing certificates. CertMagic has a 
[Storage Interface](https://pkg.go.dev/github.com/caddyserver/certmagic?tab=doc#Storage) 
allowing for multiple storage implementations. 

This package is an implementation of the Storage interface that uses DynamoDB for certificate
storage. We created this implementation for use in a clustered environment where our application
runs in containers behind a load-balancer with no shared filesystem.

## Authentication with AWS

This package uses the AWS Go library for interactions with DynamoDB. As such that library can 
detect your AWS credentials in multiple ways, in the following order:

1. Environment Variables
2. Shared Credentials file
3. Shared Configuration file (if SharedConfig is enabled)
4. EC2 Instance Metadata (credentials only)

For more information about authentication see https://docs.aws.amazon.com/sdk-for-go/api/aws/session/.

## Usage
```go
package whatever

import (
    "github.com/caddyserver/certmagic"

    dynamodbstore "github.com/silinternational/certmagic-storage-dynamodb/v3"
)

// ...

certmagic.Default.Storage = &dynamodbstore.DynamoDBStorage{
    Table:               "CertMagic",
    LockTimeout:         2 * time.Minute, // optional: default is 5 minutes
    LockPollingInterval: 2 * time.Second, // optional: default is 5 seconds
}

// ...
```
Only the table name is required, but you can also override the default values for `LockTimeout` and 
`LockPollingTimeout` if you want. Technically you can also override `AwsEndpoint`, `AwsRegion`, and 
`AwsDisableSSL` if you are running your own DynamoDB service. These settings are used in the unit tests
so you can look there for examples. 

## Testing locally
You can build and run the tests for this package locally so long as you have Docker and Docker Compose
available. Just run `docker-compose run test`. You could also run the DynamoDB local service separately 
and just run the tests from your local system, just be sure to adjust the `AWS_ENDPOINT` environment 
variable to point to where you have `dynamodb-local` running. 

## Creating the DynamoDB Table 

### Command line:
```
aws dynamodb create-table \
    --table-name CertMagic \
    --billing-mode PAY_PER_REQUEST \
    --attribute-definitions AttributeName=PrimaryKey,AttributeType=S \
    --key-schema AttributeName=PrimaryKey,KeyType=HASH
```

### Terraform
```hcl
resource "aws_dynamodb_table" "CertMagic" {
  name           = "CertMagic"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "PrimaryKey"

  attribute {
    name = "PrimaryKey"
    type = "S"
  }
}
```

## Contributing
Please do, we like reported issues and pull requests. 

## License
MIT License

Copyright (c) 2022 SIL International

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
