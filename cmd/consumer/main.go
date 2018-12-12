package main

import (
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type sampleRecordProcessor struct {
	checkpointer   kcl.Checkpointer
	checkpointFreq time.Duration
	largestPair    kcl.SequencePair
	lastCheckpoint time.Time
}

func newSampleRecordProcessor() *sampleRecordProcessor {
	return &sampleRecordProcessor{
		checkpointFreq: 60 * time.Second,
	}
}

func (srp *sampleRecordProcessor) Initialize(shardID string, checkpointer kcl.Checkpointer) error {
	srp.lastCheckpoint = time.Now()
	srp.checkpointer = checkpointer
	return nil
}

func (srp *sampleRecordProcessor) shouldUpdateSequence(pair kcl.SequencePair) bool {
	return srp.largestPair.IsNil() || srp.largestPair.IsLessThan(pair)
}

func (srp *sampleRecordProcessor) ProcessRecords(records []kcl.Record) error {
	for _, record := range records {
		seqNumber := new(big.Int)
		if _, ok := seqNumber.SetString(record.SequenceNumber, 10); !ok {
			fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", record.SequenceNumber)
			continue
		}
		pair := kcl.SequencePair{Sequence: seqNumber, SubSequence: record.SubSequenceNumber}
		if srp.shouldUpdateSequence(pair) {
			srp.largestPair = pair
		}
	}
	if time.Now().Sub(srp.lastCheckpoint) > srp.checkpointFreq {
		srp.checkpointer.Checkpoint(srp.largestPair)
		srp.lastCheckpoint = time.Now()
	}
	return nil
}

func (srp *sampleRecordProcessor) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		srp.checkpointer.Shutdown()
	} else if reason == "SHUTDOWN_REQUESTED" {
		fmt.Fprintf(os.Stderr, "Got shutdown requested, attempt to checkpoint.\n")
		srp.checkpointer.Shutdown()
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Will not checkpoint.\n")
	}
	return nil
}

func main() {
	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, newSampleRecordProcessor())
	kclProcess.Run()
}
