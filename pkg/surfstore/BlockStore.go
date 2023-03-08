package surfstore

import (
	context "context"
	"fmt"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	sync "sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mtx      sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	if _, ok := bs.BlockMap[blockHash.Hash]; !ok {
		return nil, fmt.Errorf("block not found: %v", blockHash.Hash)
	}
	return bs.BlockMap[blockHash.Hash], nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashes []string
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			hashes = append(hashes, hash)
		}
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := make([]string, 0, len(bs.BlockMap))
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	for k := range bs.BlockMap {
		hashes = append(hashes, k)
	}
	return &BlockHashes{Hashes: hashes}, nil
}
