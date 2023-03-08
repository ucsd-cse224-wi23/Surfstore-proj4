package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println(err)
		return
	}

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println(err)
		return
	}

	hashMap, err := syncLocalIndex(client, &localIndex, files)
	if err != nil {
		log.Println(err)
		return
	}

	if err = checkDeletedFiles(&localIndex, hashMap); err != nil {
		log.Println(err)
		return
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		log.Println(err)
		return
	}

	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		log.Println(err)
		return
	}
	log.Println("blockStoreAddrs: ", blockStoreAddrs)

	if err = uploadNewFiles(client, &localIndex, &remoteIndex, blockStoreAddrs); err != nil {
		log.Println(err)
		return
	}

	if err = downloadNewFiles(client, &localIndex, &remoteIndex, blockStoreAddrs); err != nil {
		log.Println(err)
		return
	}
	WriteMetaFile(localIndex, client.BaseDir)
}

func downloadNewFiles(client RPCClient, localIndex *map[string]*FileMetaData, remoteIndex *map[string]*FileMetaData, blockStoreAddrs []string) error {
	for filename, remoteMetaData := range *remoteIndex {

		if localMetaData, ok := (*localIndex)[filename]; ok {
			// local version is lower
			if localMetaData.Version < remoteMetaData.Version || (localMetaData.Version == remoteMetaData.Version && !reflect.DeepEqual(localMetaData.BlockHashList, remoteMetaData.BlockHashList)) {
				if err := downloadFile(client, localMetaData, remoteMetaData, blockStoreAddrs); err != nil {
					return err
				}
			}
		} else {
			// local version not found
			(*localIndex)[filename] = &FileMetaData{}
			localMetaData := (*localIndex)[filename]
			if err := downloadFile(client, localMetaData, remoteMetaData, blockStoreAddrs); err != nil {
				return err
			}
		}
	}
	return nil
}

func downloadFile(client RPCClient, localMetaData *FileMetaData, remoteMetaData *FileMetaData, blockStoreAddrs []string) error {
	path := client.BaseDir + "/" + remoteMetaData.Filename
	file, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer file.Close()

	*localMetaData = *remoteMetaData

	//File deleted in server
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		if err := os.Remove(path); err != nil {
			log.Println("Could not remove local file: ", err)
			return err
		}
		return nil
	}

	data := ""
	for _, hash := range remoteMetaData.BlockHashList {
		c := NewConsistentHashRing(blockStoreAddrs)
		blockStoreAddr := c.GetResponsibleServer(hash)

		var block Block
		if err := client.GetBlock(hash, blockStoreAddr, &block); err != nil {
			log.Println("Failed to get block: ", err)
		}

		data += string(block.BlockData)
	}
	file.WriteString(data)

	return nil
}

func uploadNewFiles(client RPCClient, localIndex *map[string]*FileMetaData, remoteIndex *map[string]*FileMetaData, blockStoreAddrs []string) error {
	//Check if server has locas files, upload changes
	for fileName, localMetaData := range *localIndex {
		if remoteMetaData, ok := (*remoteIndex)[fileName]; ok {
			// find a lower version file in remote
			if remoteMetaData.Version < localMetaData.Version {
				err := uploadFile(client, localMetaData, blockStoreAddrs)
				if err != nil {
					return err
				}
			}
		} else {
			// file not found in remote
			err := uploadFile(client, localMetaData, blockStoreAddrs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func uploadFile(client RPCClient, localMetaData *FileMetaData, blockStoreAddrs []string) error {
	// todo: upload blocks to their own blockstore
	path := client.BaseDir + "/" + localMetaData.Filename

	var latestVersion int32
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(localMetaData, &latestVersion)
		if err != nil {
			log.Println("Could not upload file: ", err)
		}
		localMetaData.Version = latestVersion
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	fileStat, _ := os.Stat(path)
	var numBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numBlocks; i++ {
		byteSlice := make([]byte, client.BlockSize)
		len, err := file.Read(byteSlice)
		if err != nil && err != io.EOF {
			log.Println("Error reading bytes from file in basedir: ", err)
		}
		byteSlice = byteSlice[:len]
		block := Block{BlockData: byteSlice, BlockSize: int32(len)}

		hash := sha256.New()
		hash.Write(byteSlice)
		hashBytes := hash.Sum(nil)
		hashCode := hex.EncodeToString(hashBytes)

		c := NewConsistentHashRing(blockStoreAddrs)
		blockStoreAddr := c.GetResponsibleServer(hashCode)
		//blockStoreAddr := getBlockAddr(hashCode, blockStoreAddrs)
		log.Println("upload blockStoreAddr: ", blockStoreAddr)

		var succ bool
		if err := client.PutBlock(&block, blockStoreAddr, &succ); err != nil {
			log.Println("Failed to put block: ", err)
		}
	}

	if err := client.UpdateFile(localMetaData, &latestVersion); err != nil {
		log.Println("Failed to update file: ", err)
		localMetaData.Version = -1
	}
	localMetaData.Version = latestVersion

	return nil
}

func checkDeletedFiles(localIndex *map[string]*FileMetaData, hashMap map[string][]string) error {
	// deleted files
	for file, metaData := range *localIndex {
		if _, ok := hashMap[file]; !ok {
			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" {
				metaData.Version++
				metaData.BlockHashList = []string{"0"}
			}
		}
	}
	return nil
}

func syncLocalIndex(client RPCClient, localIndex *map[string]*FileMetaData, files []fs.FileInfo) (hashMap map[string][]string, err error) {
	hashMap = make(map[string][]string)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		var numBlocks int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		fileToRead, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Println(err)
			return nil, err
		}

		for i := 0; i < numBlocks; i++ {
			byteSlice := make([]byte, client.BlockSize)
			n, err := fileToRead.Read(byteSlice)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			byteSlice = byteSlice[:n]
			hash := GetBlockHashString(byteSlice)
			hashMap[file.Name()] = append(hashMap[file.Name()], hash)
		}

		if val, ok := (*localIndex)[file.Name()]; ok {
			if !reflect.DeepEqual(hashMap[file.Name()], val.BlockHashList) {
				(*localIndex)[file.Name()].BlockHashList = hashMap[file.Name()]
				(*localIndex)[file.Name()].Version += 1
			}
		} else {
			newMetaFile := FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: hashMap[file.Name()]}
			(*localIndex)[file.Name()] = &newMetaFile
		}
	}

	return hashMap, nil
}
