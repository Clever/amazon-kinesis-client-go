package kcl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type RecordProcessor interface {
	Initialize(shardID string, checkpointer Checkpointer) error
	ProcessRecords(records []Record) error
	Shutdown(reason string) error
}

type Checkpointer interface {
	Checkpoint(pair SequencePair, retryCount int) error
	Shutdown()
}

type CheckpointError struct {
	e string
}

func (ce CheckpointError) Error() string {
	return ce.e
}

type ioHandler struct {
	inputFile  io.Reader
	outputFile io.Writer
	errorFile  io.Writer
}

//func newIOHandler(inputFile io.Reader, outputFile io.Writer, errorFile io.)

func (i ioHandler) writeLine(line string) {
	fmt.Fprintf(i.outputFile, "\n%s\n", line)
}

func (i ioHandler) writeError(message string) {
	fmt.Fprintf(i.errorFile, "%s\n", message)
}

func (i ioHandler) readLine() (string, error) {
	bio := bufio.NewReader(i.inputFile)
	line, err := bio.ReadString('\n')
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
	i := ioHandler{
		inputFile:  inputFile,
		outputFile: outputFile,
		errorFile:  errorFile,
	}
	return &KCLProcess{
		ioHandler:       i,
		recordProcessor: recordProcessor,

		checkpoint:    make(chan SequencePair),
		checkpointErr: make(chan error),
	}
}

type KCLProcess struct {
	ckpmux  sync.Mutex
	readmux sync.Mutex

	ioHandler       ioHandler
	recordProcessor RecordProcessor

	checkpoint    chan SequencePair
	checkpointErr chan error
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

func (kclp *KCLProcess) performAction(a interface{}) (string, error) {
	switch action := a.(type) {
	case ActionInitialize:
		return action.Action, kclp.recordProcessor.Initialize(action.ShardID, kclp)
	case ActionProcessRecords:
		return action.Action, kclp.recordProcessor.ProcessRecords(action.Records)
	case ActionShutdown:
		return action.Action, kclp.recordProcessor.Shutdown(action.Reason)
	default:
		return "", fmt.Errorf("unknown action to dispatch: %+#v", action)
	}
}

func (kclp *KCLProcess) handleLine(line string) error {
	kclp.readmux.Lock()
	defer kclp.readmux.Unlock()

	action, err := kclp.ioHandler.loadAction(line)
	if err != nil {
		return err
	}

	responseFor, err := kclp.performAction(action)
	if err != nil {
		return err
	}
	return kclp.reportDone(responseFor)
}

func (kclp *KCLProcess) Checkpoint(pair SequencePair, retryCount int) error {
	sleepDuration := 5 * time.Second

	for n := 0; n <= retryCount; n++ {
		err := kclp.processCheckpoint(pair)
		if err == nil {
			return nil
		}

		if cperr, ok := err.(CheckpointError); ok {
			switch cperr.Error() {
			case "ShutdownException":
				return fmt.Errorf("Encountered shutdown exception, skipping checkpoint")
			case "ThrottlingException":
				fmt.Fprintf(os.Stderr, "Checkpointing throttling, pause for %s\n", sleepDuration)
			case "InvalidStateException":
				fmt.Fprintf(os.Stderr, "MultiLangDaemon invalid state while checkpointing\n")
			default:
				fmt.Fprintf(os.Stderr, "Encountered an error while checkpointing: %s", err)
			}
		}

		if n == retryCount {
			return fmt.Errorf("Failed to checkpoint after %d attempts, giving up.", retryCount)
		}

		time.Sleep(sleepDuration)
	}

	return nil
}

func (kclp *KCLProcess) Shutdown() {
	kclp.Checkpoint(SequencePair{}, 5)
}

func (kclp *KCLProcess) processCheckpoint(pair SequencePair) error {
	kclp.ckpmux.Lock()
	defer kclp.ckpmux.Unlock()

	var seq *string
	var subSeq *int
	if !pair.IsEmpty() { // an empty pair is a signal to shutdown
		tmp := pair.Sequence.String()
		seq = &tmp
		subSeq = &pair.SubSequence
	}
	kclp.ioHandler.writeAction(ActionCheckpoint{
		Action:            "checkpoint",
		SequenceNumber:    seq,
		SubSequenceNumber: subSeq,
	})
	line, err := kclp.ioHandler.readLine()
	if err != nil {
		return err
	}
	actionI, err := kclp.ioHandler.loadAction(line)
	if err != nil {
		return err
	}
	action, ok := actionI.(ActionCheckpoint)
	if !ok {
		return fmt.Errorf("expected checkpoint response, got '%s'", line)
	}
	if action.Error != nil && *action.Error != "" {
		return CheckpointError{e: *action.Error}
	}
	return nil
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
			return
		}

		err = kclp.handleLine(line)
		if err != nil {
			kclp.ioHandler.writeError(fmt.Sprintf("ERR Handle line: %+#v", err))
			return
		}
	}
}
