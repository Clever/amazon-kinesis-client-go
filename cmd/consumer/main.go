package main

import (
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type SampleRecordProcessor struct {
	sleepDuration     time.Duration
	checkpointRetries int
	checkpointFreq    time.Duration
	largestSeq        *big.Int
	largestSubSeq     int
	lastCheckpoint    time.Time
}

func New() *SampleRecordProcessor {
	return &SampleRecordProcessor{
		sleepDuration:     5 * time.Second,
		checkpointRetries: 5,
		checkpointFreq:    60 * time.Second,
	}
}

func (srp *SampleRecordProcessor) Initialize(shardID string) error {
	srp.lastCheckpoint = time.Now()
	return nil
}

func (srp *SampleRecordProcessor) checkpoint(checkpointer kcl.Checkpointer, sequenceNumber string, subSequenceNumber int) {
	for n := -1; n < srp.checkpointRetries; n++ {
		err := checkpointer.Checkpoint(sequenceNumber, subSequenceNumber)
		if err == nil {
			return
		}

		if cperr, ok := err.(kcl.CheckpointError); ok {
			switch cperr.Error() {
			case "ShutdownException":
				fmt.Fprintf(os.Stderr, "Encountered shutdown exception, skipping checkpoint\n")
				return
			case "ThrottlingException":
				fmt.Fprintf(os.Stderr, "Was throttled while checkpointing, will attempt again in %s", srp.sleepDuration)
			case "InvalidStateException":
				fmt.Fprintf(os.Stderr, "MultiLangDaemon reported an invalid state while checkpointing\n")
			default:
				fmt.Fprintf(os.Stderr, "Encountered an error while checkpointing: %s", err)
			}
		}

		if n == srp.checkpointRetries {
			fmt.Fprintf(os.Stderr, "Failed to checkpoint after %d attempts, giving up.\n", srp.checkpointRetries)
			return
		}

		time.Sleep(srp.sleepDuration)
	}
}

func (srp *SampleRecordProcessor) shouldUpdateSequence(sequenceNumber *big.Int, subSequenceNumber int) bool {
	return srp.largestSeq == nil || sequenceNumber.Cmp(srp.largestSeq) == 1 ||
		(sequenceNumber.Cmp(srp.largestSeq) == 0 && subSequenceNumber > srp.largestSubSeq)
}

func (srp *SampleRecordProcessor) ProcessRecords(records []kcl.Record, checkpointer kcl.Checkpointer) error {
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
		srp.checkpoint(checkpointer, srp.largestSeq.String(), srp.largestSubSeq)
		srp.lastCheckpoint = time.Now()
	}
	return nil
}

func (srp *SampleRecordProcessor) Shutdown(checkpointer kcl.Checkpointer, reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		srp.checkpoint(checkpointer, "", 0)
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
