include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

SHELL := /bin/bash
JAR_DIR := jars
PKG := github.com/Clever/amazon-kinesis-client-go
PKGS := $(shell go list ./... | grep -v /vendor )
.PHONY: download_jars run build

URL_PREFIX := http://search.maven.org/remotecontent?filepath=

# this list lifted from https://github.com/awslabs/amazon-kinesis-client-python/blob/fb49c6390c0593fbcf81d6c34c5245726c15b2f3/setup.py#L60
JARS_TO_DOWNLOAD := $(addprefix $(JAR_DIR)/,com/amazonaws/amazon-kinesis-client/1.7.2/amazon-kinesis-client-1.7.2.jar \
com/amazonaws/aws-java-sdk-dynamodb/1.11.14/aws-java-sdk-dynamodb-1.11.14.jar \
com/amazonaws/aws-java-sdk-s3/1.11.14/aws-java-sdk-s3-1.11.14.jar \
com/amazonaws/aws-java-sdk-kms/1.11.14/aws-java-sdk-kms-1.11.14.jar \
com/amazonaws/aws-java-sdk-core/1.11.14/aws-java-sdk-core-1.11.14.jar \
commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar \
org/apache/httpcomponents/httpclient/4.5.2/httpclient-4.5.2.jar \
org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar \
commons-codec/commons-codec/1.9/commons-codec-1.9.jar \
com/fasterxml/jackson/core/jackson-databind/2.6.6/jackson-databind-2.6.6.jar \
com/fasterxml/jackson/core/jackson-annotations/2.6.0/jackson-annotations-2.6.0.jar \
com/fasterxml/jackson/core/jackson-core/2.6.6/jackson-core-2.6.6.jar \
com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.6/jackson-dataformat-cbor-2.6.6.jar \
joda-time/joda-time/2.8.1/joda-time-2.8.1.jar \
com/amazonaws/aws-java-sdk-kinesis/1.11.14/aws-java-sdk-kinesis-1.11.14.jar \
com/amazonaws/aws-java-sdk-cloudwatch/1.11.14/aws-java-sdk-cloudwatch-1.11.14.jar \
com/google/guava/guava/18.0/guava-18.0.jar \
com/google/protobuf/protobuf-java/2.6.1/protobuf-java-2.6.1.jar \
commons-lang/commons-lang/2.6/commons-lang-2.6.jar)

EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
JAVA_CLASS_PATH := $(subst $(SPACE),:,$(JARS_TO_DOWNLOAD))

CONSUMER ?= consumer

$(JARS_TO_DOWNLOAD):
	mkdir -p `dirname $@`
	curl -s -L -o $@ -O $(URL_PREFIX)`echo $@ | sed 's/$(JAR_DIR)\///g'`

download_jars: $(JARS_TO_DOWNLOAD)

build:
	CGO_ENABLED=0 go build -installsuffix cgo -o build/$(CONSUMER) $(PKG)/cmd/$(CONSUMER)

run: build download_jars
	command -v java >/dev/null 2>&1 || { echo >&2 "Java not installed. Install java!"; exit 1; }
	java -cp $(JAVA_CLASS_PATH) \
	com.amazonaws.services.kinesis.multilang.MultiLangDaemon \
	$(CONSUMER).properties

bench:
	go test -bench=. github.com/Clever/amazon-kinesis-client-go/decode/

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)
