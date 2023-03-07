package surfstore

import (
	context "context"
	log "log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		log.Println(err)
		return err
	}

	*blockHashes = hashes.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Dial error: ", err)
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := c.PutBlock(ctx, block)
	if err != nil || !s.Flag {
		conn.Close()
		return err
	}
	*succ = s.Flag

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		log.Println(err)
		return err
	}

	*blockHashesOut = hashes.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		return err
	}
	c := NewMetaStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mp, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		log.Println(err)
		return err
	}

	*serverFileInfoMap = mp.FileInfoMap

	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		log.Println(err)
		return err
	}
	c := NewMetaStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		log.Println(err)
		return err
	}

	*latestVersion = v.Version

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// todo: implement

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// todo: implement
	panic("todo")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
