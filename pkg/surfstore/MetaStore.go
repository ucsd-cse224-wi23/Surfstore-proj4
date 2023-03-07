package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	log "log"
	sync "sync"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	mtx                sync.Mutex
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	m.mtx.Lock()
	defer m.mtx.Unlock()
	log.Println("fileName: ", filename)

	if _, ok := m.FileMetaMap[filename]; ok {
		currentVersion := m.FileMetaMap[filename].Version
		if version == currentVersion+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			return &Version{Version: -1}, nil
		}
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	}

	return &Version{Version: version}, nil
}

// Given a list of block hashes, find out which block server they belong to. Returns a mapping from block server address to block hashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// todo: implement

}

// Returns all the BlockStore addresses.
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// todo: implement

}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
