# amazon-kinesis-client-go

A port of [amazon-kinesis-client-python](https://github.com/awslabs/amazon-kinesis-client-python) to Go.

## Running the Consumer

Edit the file `consumer.properties` to point at a Kinesis stream that has some data.
Build the consumer binary:

``` bash
make build
```

Then run:

``` bash
make run
```

This will download the jar files necessary to run the KCL, and then launch the KCL communicating with the consumer binary.
