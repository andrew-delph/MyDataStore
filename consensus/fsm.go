package consensus

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/andrew-delph/my-key-store/datap"
)

var applyLock sync.RWMutex

var snapshotLock sync.RWMutex

type FSM struct {
	data  *datap.Fsm
	reqCh chan interface{}
	index *uint64
}

func (fsm *FSM) Apply(logEntry *raft.Log) interface{} {
	applyLock.Lock()
	defer applyLock.Unlock()

	data := &datap.Fsm{}
	err := proto.Unmarshal(logEntry.Data, data)
	if err != nil {
		return err
	}
	fsm.data = data

	if fsm.data.Epoch > data.Epoch {
		logrus.Warnf("epoch is less fsm %d new %d index %d old %d", fsm.data.Epoch, data.Epoch, logEntry.Index, *fsm.index)
		return nil
	}

	resCh := make(chan interface{})
	fsm.reqCh <- FsmTask{Epoch: data.Epoch, ResCh: resCh, Members: data.Members, TempMembers: data.TempMembers}
	rawRes := <-resCh

	logrus.Debug("rawRes %v", rawRes)

	return nil
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// logrus.Warnf("Snapshot start")
	// defer logrus.Warnf("Snapshot done")
	snapshotLock.Lock()
	defer snapshotLock.Unlock()

	dataBytes, err := proto.Marshal(fsm.data)
	if err != nil {
		return nil, err
	}

	return &FSMSnapshot{DataBytes: dataBytes}, nil
}

func (fsm *FSM) Restore(serialized io.ReadCloser) error {
	applyLock.Lock()
	defer applyLock.Unlock()
	var snapshot FSMSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	data := &datap.Fsm{}
	err := proto.Unmarshal(snapshot.DataBytes, data)
	if err != nil {
		return err
	}

	fsm.data = data
	resCh := make(chan interface{})
	fsm.reqCh <- FsmTask{Epoch: data.Epoch, ResCh: resCh, Members: data.Members, TempMembers: data.TempMembers}
	rawRes := <-resCh

	logrus.Debug("rawRes %v", rawRes)

	logrus.Warnf("Restore raft. Epoch = %v", data.Epoch)

	return serialized.Close()
}

type FSMSnapshot struct {
	DataBytes []byte `json:"value"`
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
