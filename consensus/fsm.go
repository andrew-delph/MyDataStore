package consensus

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/andrew-delph/my-key-store/utils"
)

var applyLock sync.RWMutex

var snapshotLock sync.RWMutex

type FSM struct {
	Epoch int64
	reqCh chan interface{}
	index *uint64
}

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	applyLock.Lock()
	defer applyLock.Unlock()
	*fsm.index = logEntry.Index
	epoch, err := utils.DecodeBytesToInt64(logEntry.Data)
	if err != nil {
		logrus.Error("DecodeBytesToInt64 Error on Apply: %v", err)
		return nil
	}
	fsm.Epoch = epoch

	fsm.reqCh <- EpochTask{Epoch: epoch}

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
