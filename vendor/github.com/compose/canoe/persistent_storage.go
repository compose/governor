package canoe

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

type walMetadata struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
}

func (rn *Node) initPersistentStorage() error {
	if err := rn.initSnap(); err != nil {
		return err
	}

	raftSnap, err := rn.ss.Load()
	if err != nil {
		if err != snap.ErrNoSnapshot && err != snap.ErrEmptySnapshot {
			return err
		}
	}

	var walSnap walpb.Snapshot

	if raftSnap != nil {
		walSnap.Index, walSnap.Term = raftSnap.Metadata.Index, raftSnap.Metadata.Term
	}

	if err := rn.initWAL(walSnap); err != nil {
		return err
	}

	return nil
}

// Correct order of ops
// 1: Restore Metadata from WAL
// 2: Apply any Snapshot to raft storage
// 3: Apply any hardstate to raft storage
// 4: Apply and WAL Entries to raft storage
// 5: Apply any persisted snapshot to FSM
// 6: Apply any persisted WAL data to FSM
func (rn *Node) restoreRaft() error {
	raftSnap, err := rn.ss.Load()
	if err != nil {
		if err != snap.ErrNoSnapshot && err != snap.ErrEmptySnapshot {
			return err
		}
	}

	var walSnap walpb.Snapshot

	if raftSnap != nil {
		walSnap.Index, walSnap.Term = raftSnap.Metadata.Index, raftSnap.Metadata.Term
	} else {
		raftSnap = &raftpb.Snapshot{}
	}

	wMetadata, hState, ents, err := rn.wal.ReadAll()
	if err != nil {
		return err
	}

	// NOTE: Step 1
	if err := rn.restoreMetadata(wMetadata); err != nil {
		return err
	}

	// We can do this now that we restored the metadata
	if err := rn.attachTransport(); err != nil {
		return err
	}

	if err := rn.transport.Start(); err != nil {
		return err
	}

	// NOTE: Step 2, 3, 4
	if err := rn.restoreMemoryStorage(*raftSnap, hState, ents); err != nil {
		return err
	}

	// NOTE: Step 5
	if err := rn.restoreFSMFromSnapshot(*raftSnap); err != nil {
		return err
	}

	// NOTE: Step 6
	if err := rn.restoreFSMFromWAL(ents); err != nil {
		return err
	}

	return nil
}

func (rn *Node) initSnap() error {
	if rn.snapDir() == "" {
		return nil
	}

	if err := os.MkdirAll(rn.snapDir(), 0750); err != nil && !os.IsExist(err) {
		return err
	}

	rn.ss = snap.New(rn.snapDir())

	return nil
}

func (rn *Node) persistSnapshot(raftSnap raftpb.Snapshot) error {

	if rn.ss != nil {
		if err := rn.ss.SaveSnap(raftSnap); err != nil {
			return err
		}
	}

	if rn.wal != nil {
		var walSnap walpb.Snapshot
		walSnap.Index, walSnap.Term = raftSnap.Metadata.Index, raftSnap.Metadata.Term

		if err := rn.wal.SaveSnapshot(walSnap); err != nil {
			return err
		}
	}
	return nil
}

func (rn *Node) initWAL(walSnap walpb.Snapshot) error {
	if rn.walDir() == "" {
		return nil
	}

	if !wal.Exist(rn.walDir()) {

		if err := os.MkdirAll(rn.walDir(), 0750); err != nil && !os.IsExist(err) {
			return err
		}

		metaStruct := &walMetadata{
			NodeID:    rn.id,
			ClusterID: rn.cid,
		}

		metaData, err := json.Marshal(metaStruct)
		if err != nil {
			return err
		}

		w, err := wal.Create(rn.walDir(), metaData)
		if err != nil {
			return err
		}
		rn.wal = w
	} else {
		// This assumes we WILL be reading this once elsewhere
		w, err := wal.Open(rn.walDir(), walSnap)
		if err != nil {
			return err
		}
		rn.wal = w
	}

	return nil
}

func (rn *Node) restoreMetadata(wMetadata []byte) error {
	var metaData walMetadata
	if err := json.Unmarshal(wMetadata, &metaData); err != nil {
		return err
	}

	rn.id, rn.cid = metaData.NodeID, metaData.ClusterID
	rn.raftConfig.ID = metaData.NodeID
	return nil
}

// restores FSM AND it sets the NodeID and ClusterID if present in Metadata
func (rn *Node) restoreFSMFromWAL(ents []raftpb.Entry) error {
	if rn.wal == nil {
		return nil
	}

	if err := rn.publishEntries(ents); err != nil {
		return err
	}

	return nil
}

func (rn *Node) restoreMemoryStorage(raftSnap raftpb.Snapshot, hState raftpb.HardState, ents []raftpb.Entry) error {
	if !raft.IsEmptySnap(raftSnap) {
		if err := rn.raftStorage.ApplySnapshot(raftSnap); err != nil {
			return err
		}
	}

	if rn.wal != nil {
		if err := rn.raftStorage.SetHardState(hState); err != nil {
			return err
		}

		if err := rn.raftStorage.Append(ents); err != nil {
			return err
		}
	}

	return nil
}

func (rn *Node) deletePersistentData() error {
	if rn.snapDir() != "" {
		if err := os.RemoveAll(rn.snapDir()); err != nil {
			return err
		}
	}
	if rn.walDir() != "" {
		if err := os.RemoveAll(rn.snapDir()); err != nil {
			return err
		}
	}
	return nil
}

func (rn *Node) walDir() string {
	if rn.dataDir == "" {
		return ""
	}
	return fmt.Sprintf("%s%s", rn.dataDir, walDirExtension)
}

func (rn *Node) snapDir() string {
	if rn.dataDir == "" {
		return ""
	}
	return fmt.Sprintf("%s%s", rn.dataDir, snapDirExtension)
}
