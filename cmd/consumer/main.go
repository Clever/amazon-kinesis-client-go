package main

import (
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type SampleRecordProcessor struct {
	checkpointer      *kcl.Checkpointer
	checkpointRetries int
	checkpointFreq    time.Duration
	largestSeq        *big.Int
	largestSubSeq     int
	lastCheckpoint    time.Time
}

func New() *SampleRecordProcessor {
	return &SampleRecordProcessor{
		checkpointRetries: 5,
		checkpointFreq:    60 * time.Second,
	}
}

func (srp *SampleRecordProcessor) Initialize(shardID string, checkpointer *kcl.Checkpointer) error {
	srp.lastCheckpoint = time.Now()
	srp.checkpointer = checkpointer
	return nil
}

func (srp *SampleRecordProcessor) shouldUpdateSequence(sequenceNumber *big.Int, subSequenceNumber int) bool {
	return srp.largestSeq == nil || sequenceNumber.Cmp(srp.largestSeq) == 1 ||
		(sequenceNumber.Cmp(srp.largestSeq) == 0 && subSequenceNumber > srp.largestSubSeq)
}

func (srp *SampleRecordProcessor) ProcessRecords(records []kcl.Record) error {
	for _, record := range records {
		seqNumber := new(big.Int)
		if _, ok := seqNumber.SetString(record.SequenceNumber, 10); !ok {
			fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", record.SequenceNumber)
			continue
		}
		if srp.shouldUpdateSequence(seqNumber, record.SubSequenceNumber) {
			srp.largestSeq = seqNumber
			srp.largestSubSeq = record.SubSequenceNumber
		}
	}
	if time.Now().Sub(srp.lastCheckpoint) > srp.checkpointFreq {
		largestSeq := srp.largestSeq.String()
		srp.checkpointer.CheckpointWithRetry(&largestSeq, &srp.largestSubSeq, srp.checkpointRetries)
		srp.lastCheckpoint = time.Now()
	}
	return nil
}

func (srp *SampleRecordProcessor) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		srp.checkpointer.Shutdown()
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Will not checkpoint.\n")
	}
	return nil
}

func main() {
	f, err := os.Create("/tmp/kcl_stderr")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, New())
	kclProcess.Run()
}
