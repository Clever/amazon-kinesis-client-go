package kcl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type RecordProcessor interface {
	Initialize(shardID string, checkpointer *Checkpointer) error
	ProcessRecords(records []Record) error
	Shutdown(reason string) error
}

type CheckpointError struct {
	e string
}

func (ce CheckpointError) Error() string {
	return ce.e
}

type Checkpointer struct {
	mux sync.Mutex

	ioHandler ioHandler
}

func (c *Checkpointer) getAction() (interface{}, error) {
	line, err := c.ioHandler.readLine()
	if err != nil {
		return nil, err
	}
	action, err := c.ioHandler.loadAction(line.String())
	if err != nil {
		return nil, err
	}
	return action, nil
}

func (c *Checkpointer) Checkpoint(sequenceNumber *string, subSequenceNumber *int) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.ioHandler.writeAction(ActionCheckpoint{
		Action:            "checkpoint",
		SequenceNumber:    sequenceNumber,
		SubSequenceNumber: subSequenceNumber,
	})
	line, err := c.ioHandler.readLine()
	if err != nil {
		return err
	}
	actionI, err := c.ioHandler.loadAction(line.String())
	if err != nil {
		return err
	}
	action, ok := actionI.(ActionCheckpoint)
	if !ok {
		return fmt.Errorf("expected checkpoint response, got '%s'", line.String())
	}
	if action.Error != nil && *action.Error != "" {
		return CheckpointError{
			e: *action.Error,
		}
	}
	return nil
}

// CheckpointWithRetry tries to save a checkPoint up to `retryCount` + 1 times.
// `retryCount` should be >= 0
func (c *Checkpointer) CheckpointWithRetry(
	sequenceNumber *string, subSequenceNumber *int, retryCount int,
) error {
	sleepDuration := 5 * time.Second

	for n := 0; n <= retryCount; n++ {
		err := c.Checkpoint(sequenceNumber, subSequenceNumber)
		if err == nil {
			return nil
		}

		if cperr, ok := err.(CheckpointError); ok {
			switch cperr.Error() {
			case "ShutdownException":
				return fmt.Errorf("Encountered shutdown exception, skipping checkpoint")
			case "ThrottlingException":
				fmt.Fprintf(os.Stderr, "Was throttled while checkpointing, will attempt again in %s\n", sleepDuration)
			case "InvalidStateException":
				fmt.Fprintf(os.Stderr, "MultiLangDaemon reported an invalid state while checkpointing\n")
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

func (c *Checkpointer) Shutdown() {
	c.CheckpointWithRetry(nil, nil, 5)
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

func (i ioHandler) readLine() (*bytes.Buffer, error) {
	bio := bufio.NewReader(i.inputFile)
	line, err := bio.ReadString('\n')
	if err != nil {
		return nil, err
	}
	return bytes.NewBufferString(line), nil
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

func New(inputFile io.Reader, outputFile, errorFile io.Writer, recordProcessor RecordProcessor) *KCLProcess {
	i := ioHandler{
		inputFile:  inputFile,
		outputFile: outputFile,
		errorFile:  errorFile,
	}
	return &KCLProcess{
		ioHandler: i,
		checkpointer: &Checkpointer{
			ioHandler: i,
		},
		recordProcessor: recordProcessor,
	}
}

type KCLProcess struct {
	ioHandler       ioHandler
	checkpointer    *Checkpointer
	recordProcessor RecordProcessor
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
		return action.Action, kclp.recordProcessor.Initialize(action.ShardID, kclp.checkpointer)
	case ActionProcessRecords:
		return action.Action, kclp.recordProcessor.ProcessRecords(action.Records)
	case ActionShutdown:
		return action.Action, kclp.recordProcessor.Shutdown(action.Reason)
	default:
		return "", fmt.Errorf("unknown action to dispatch: %s", action)
	}
}

func (kclp *KCLProcess) handleLine(line string) error {
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

func (kclp *KCLProcess) Run() {
	for {
		line, err := kclp.ioHandler.readLine()
		if err != nil {
			kclp.ioHandler.writeError(err.Error())
			return
		} else if line == nil {
			break
		}
		kclp.handleLine(line.String())
	}
}
