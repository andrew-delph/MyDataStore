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
	epoch, err := utils.DecodeBytesToInt64(logEntry.Data)
	if err != nil {
		logrus.Error("DecodeBytesToInt64 Error on Apply: %v", err)
		return nil
	}

	if fsm.Epoch > epoch {
		logrus.Warnf("epoch is less fsm %d new %d index %d old %d", fsm.Epoch, epoch, logEntry.Index, *fsm.index)
		return nil
	}
	fsm.Epoch = epoch
	*fsm.index = logEntry.Index
	resCh := make(chan interface{})
	fsm.reqCh <- EpochTask{Epoch: epoch, ResCh: resCh}
	rawRes := <-resCh

	logrus.Debugf("Apply res: %v", rawRes)

	return epoch
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// logrus.Warnf("Snapshot start")
	// defer logrus.Warnf("Snapshot done")
	snapshotLock.Lock()
	defer snapshotLock.Unlock()

	return &FSMSnapshot{StateValue: fsm.Epoch}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	fsm.Epoch = snapshot.StateValue
	logrus.Warnf("Restore raft %d", snapshot.StateValue)

	return serialized.Close()
}

type FSMSnapshot struct {
	StateValue int64 `json:"value"`
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
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
		logrus.Errorf("Persist ERROR = %v", err)
		sink.Cancel()
		return err
	}

	return nil
}

func (fsmSnapshot *FSMSnapshot) Release() {
}
