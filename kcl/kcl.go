package kcl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

type RecordProcessor interface {
	Initialize(shardID string, checkpointer Checkpointer) error
	ProcessRecords(records []Record) error
	// Shutdown this call should block until it's safe to shutdown the process
	Shutdown(reason string) error
}

type Checkpointer interface {
	Checkpoint(pair SequencePair)
	Shutdown()
}

type ioHandler struct {
	input      *bufio.Reader
	outputFile io.Writer
	errorFile  io.Writer
}

func newIOHandler(inputFile io.Reader, outputFile, errorFile io.Writer) ioHandler {
	return ioHandler{
		input:      bufio.NewReader(inputFile),
		outputFile: outputFile,
		errorFile:  errorFile,
	}
}

func (i ioHandler) writeLine(line string) {
	fmt.Fprintf(i.outputFile, "\n%s\n", line)
}

func (i ioHandler) writeError(message string) {
	fmt.Fprintf(i.errorFile, "%s\n", message)
}

func (i ioHandler) readLine() (string, error) {
	line, err := i.input.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

type ActionInitialize struct {
	Action            string `json:"action"`
	ShardID           string `json:"shardId"`
	SequenceNumber    string `json:"sequenceNumber"`
	SubSequenceNumber int    `json:"subSequenceNumber"`
}

type Record struct {
	SequenceNumber              string `json:"sequenceNumber"`
	SubSequenceNumber           int    `json:"subSequenceNumber"`
	ApproximateArrivalTimestamp int    `json:"approximateArrivalTimestamp"`
	PartitionKey                string `json:"partitionKey"`
	Data                        string `json:"data"`
}

type ActionProcessRecords struct {
	Action             string   `json:"action"`
	Records            []Record `json:"records"`
	MillisBehindLatest int      `json:"millisBehindLatest"`
}

type ActionShutdown struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type ActionCheckpoint struct {
	Action            string  `json:"action"`
	SequenceNumber    *string `json:"sequenceNumber,omitempty"`
	SubSequenceNumber *int    `json:"subSequenceNumber,omitempty"`
	Error             *string `json:"error,omitempty"`
}

func (i ioHandler) loadAction(line string) (interface{}, error) {
	lineBytes := []byte(line)
	var message struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(lineBytes, &message); err != nil {
		return nil, err
	}
	switch message.Action {
	case "initialize":
		var actionInitialize ActionInitialize
		if err := json.Unmarshal(lineBytes, &actionInitialize); err != nil {
			return nil, err
		}
		return actionInitialize, nil
	case "processRecords":
		var actionProcessRecords ActionProcessRecords
		if err := json.Unmarshal(lineBytes, &actionProcessRecords); err != nil {
			return nil, err
		}
		return actionProcessRecords, nil
	case "shutdownRequested":
		fallthrough
	case "shutdown":
		var actionShutdown ActionShutdown
		if err := json.Unmarshal(lineBytes, &actionShutdown); err != nil {
			return nil, err
		}
		return actionShutdown, nil
	case "checkpoint":
		var actionCheckpoint ActionCheckpoint
		if err := json.Unmarshal(lineBytes, &actionCheckpoint); err != nil {
			return nil, err
		}
		return actionCheckpoint, nil
	default:
		return nil, fmt.Errorf("no recognizable 'action' field in message: %s", line)
	}
}

func (i ioHandler) writeAction(action interface{}) error {
	line, err := json.Marshal(action)
	if err != nil {
		return err
	}
	i.writeLine(string(line))
	return nil
}

func New(
	inputFile io.Reader, outputFile, errorFile io.Writer, recordProcessor RecordProcessor,
) *KCLProcess {
	return &KCLProcess{
		ioHandler:       newIOHandler(inputFile, outputFile, errorFile),
		recordProcessor: recordProcessor,

		nextCheckpointPair: SequencePair{},
		doGracefulShutdown: false,
	}
}

type KCLProcess struct {
	ckpmux sync.Mutex

	ioHandler       ioHandler
	recordProcessor RecordProcessor

	nextCheckpointPair SequencePair
	doGracefulShutdown bool
}

func (kclp *KCLProcess) Checkpoint(pair SequencePair) {
	kclp.ckpmux.Lock()
	defer kclp.ckpmux.Unlock()

	if kclp.nextCheckpointPair.IsNil() || kclp.nextCheckpointPair.IsLessThan(pair) {
		kclp.nextCheckpointPair = pair
	}
}

func (kclp *KCLProcess) Shutdown() {
	kclp.ioHandler.writeError("Checkpoint shutdown")
	kclp.doGracefulShutdown = true
}

func (kclp *KCLProcess) handleCheckpointAction(action ActionCheckpoint) error {
	if action.Error == nil { // Successful checkpoint
		return nil
	}

	msg := *action.Error
	switch msg {
	case "ShutdownException":
		return fmt.Errorf("Encountered shutdown exception, skipping checkpoint")
	case "ThrottlingException":
		sleep := 5 * time.Second
		kclp.ioHandler.writeError(fmt.Sprintf("Checkpointing throttling, pause for %s", sleep))
		time.Sleep(sleep)
	case "InvalidStateException":
		kclp.ioHandler.writeError("MultiLangDaemon invalid state while checkpointing")
	default:
		kclp.ioHandler.writeError(fmt.Sprintf("Encountered an error while checkpointing: %s", msg))
	}

	seq := action.SequenceNumber
	subSeq := action.SubSequenceNumber

	kclp.ckpmux.Lock()
	if !kclp.nextCheckpointPair.IsNil() {
		tmp := kclp.nextCheckpointPair.Sequence.String()
		seq = &tmp
		subSeq = &kclp.nextCheckpointPair.SubSequence
	}
	kclp.ckpmux.Unlock()

	if seq != nil && subSeq != nil {
		return kclp.sendCheckpoint(seq, subSeq)
	}

	return nil
}

func (kclp *KCLProcess) sendCheckpoint(seq *string, subSeq *int) error {
	return kclp.ioHandler.writeAction(ActionCheckpoint{
		Action:            "checkpoint",
		SequenceNumber:    seq,
		SubSequenceNumber: subSeq,
	})
}

func (kclp *KCLProcess) reportDone(responseFor string) error {
	return kclp.ioHandler.writeAction(struct {
		Action      string `json:"action"`
		ResponseFor string `json:"responseFor"`
	}{
		Action:      "status",
		ResponseFor: responseFor,
	})
}

func (kclp *KCLProcess) handleLine(line string) (string, error) {
	action, err := kclp.ioHandler.loadAction(line)
	if err != nil {
		return "", err
	}

	switch action := action.(type) {
	case ActionCheckpoint:
		return "checkpoint", kclp.handleCheckpointAction(action)
	case ActionShutdown:
		kclp.ioHandler.writeError("Received shutdown action...")

		reason := action.Reason
		// Shutdown should block until it's safe to shutdown the process
		if action.Action == "shutdownRequested" {
			reason = "SHUTDOWN_REQUESTED"
		}
		err = kclp.recordProcessor.Shutdown(reason)

		if err != nil { // Log error and continue shutting down
			kclp.ioHandler.writeError(fmt.Sprintf("ERR shutdown: %+#v", err))
		}

		kclp.ioHandler.writeError("Reporting shutdown done")

		if kclp.doGracefulShutdown {
			err := kclp.sendCheckpoint(nil, nil) // nil sequence num is signal to shutdown
			if err != nil {
				kclp.ioHandler.writeError(fmt.Sprintf("ERR shutdown ask: %+#v", err))
			}
		}

		return "shutdown", kclp.reportDone("shutdown")
	case ActionInitialize:
		err := kclp.recordProcessor.Initialize(action.ShardID, kclp)
		if err != nil {
			return "", err
		}

		return "initialize", kclp.reportDone(action.Action)
	case ActionProcessRecords:
		err := kclp.recordProcessor.ProcessRecords(action.Records)
		if err != nil {
			return "", err
		}

		return "process-record", kclp.reportDone(action.Action)
	default:
		return "", fmt.Errorf("unknown action to dispatch: %+#v", action)
	}
}

func (kclp *KCLProcess) Run() {
	for {
		line, err := kclp.ioHandler.readLine()
		if err == io.EOF {
			kclp.ioHandler.writeError("IO stream closed")
			return
		} else if err != nil {
			kclp.ioHandler.writeError(fmt.Sprintf("ERR Read line: %+#v", err))
			return
		} else if line == "" {
			kclp.ioHandler.writeError("Empty read line recieved")
			continue
		}

		action, err := kclp.handleLine(line)
		if err != nil {
			kclp.ioHandler.writeError(fmt.Sprintf("ERR Handle line: %+#v", err))
		} else if action == "shutdown" {
			return
		}

		kclp.ckpmux.Lock()
		if !kclp.nextCheckpointPair.IsNil() {
			seq := kclp.nextCheckpointPair.Sequence.String()
			subSeq := kclp.nextCheckpointPair.SubSequence

			err := kclp.sendCheckpoint(&seq, &subSeq)
			if err != nil {
				kclp.ioHandler.writeError(fmt.Sprintf("ERR checkpoint: %+#v", err))
			} else {
				kclp.nextCheckpointPair = SequencePair{}
			}
		} else if kclp.doGracefulShutdown {
			err := kclp.sendCheckpoint(nil, nil) // nil sequence num is signal to shutdown
			if err != nil {
				kclp.ioHandler.writeError(fmt.Sprintf("ERR shutdown ask: %+#v", err))
			}
		}
		kclp.ckpmux.Unlock()
	}
}
