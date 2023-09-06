package consensus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

var applyLock sync.RWMutex

var snapshotLock sync.RWMutex

type FSM struct {
	Epoch int64
}

func EncodeInt64ToBytes(value int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeBytesToInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, errors.New("byte slice should be 8 bytes long for int64 decoding")
	}

	buf := bytes.NewReader(data)
	var result int64
	err := binary.Read(buf, binary.LittleEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	applyLock.Lock()
	defer applyLock.Unlock()
	epoch, err := DecodeBytesToInt64(logEntry.Data)
	if err != nil {
		logrus.Error("DecodeBytesToInt64 Error on Apply: %v", err)
		return nil
	}
	fsm.Epoch = epoch

	logrus.Warnf("E= %d", epoch)

	// if logEntry.Index == raftNode.AppliedIndex() {
	// 	validFSMObserver <- true
	// } else if logEntry.Index != raftNode.AppliedIndex() {
	// 	validFSMObserver <- false
	// }

	// epochObserver <- epoch

	return epoch
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// logrus.Warnf("Snapshot start")
	// defer logrus.Warnf("Snapshot done")

	snapshotLock.Lock()
	defer snapshotLock.Unlock()

	return &FSMSnapshot{stateValue: fsm.Epoch}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.Epoch = snapshot.stateValue

	return serialized.Close()
}

type FSMSnapshot struct {
	stateValue int64 `json:"value"`
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// logrus.Warn("Persist!!!!!!!!!!!!!!!!!!!!!!!!1")
	err := func() error {
		snapshotBytes, err := json.Marshal(fsmSnapshot)
		if err != nil {
			return err
		}

		if _, err := sink.Write(snapshotBytes); err != nil {
			return err
		}

		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		logrus.Warnf("Persist ERROR = %v", err)
		sink.Cancel()
		return err
	}

	return nil
}

func (fsmSnapshot *FSMSnapshot) Release() {
}
