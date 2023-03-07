package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()

	// create indexes table
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	// start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	// insert rows into indexes table
	for _, fileMeta := range fileMetas {
		for idx, hash := range fileMeta.BlockHashList {
			statement, err := tx.Prepare(insertTuple)
			if err != nil {
				log.Println(err)
			}
			statement.Exec(fileMeta.Filename, fileMeta.Version, idx, hash)
		}
	}

	// commit transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	PrintMetaMap(fileMetas)

	return nil
}

const (
	getDistinctFileName = `SELECT *
						   FROM indexes
						   WHERE fileName = ?
						   ORDER BY hashIndex ASC;`

	getTuplesByFileName = `SELECT fileName, version, hashIndex, hashValue
						   FROM indexes;`
)

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Println("Error When Opening Meta")
		return nil, err
	}
	defer db.Close()
	// Prepare the SQL statement outside of the loop
	stmt, err := db.Prepare(getTuplesByFileName)
	if err != nil {
		log.Println("Error During Meta Load")
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	allFileName := make(map[string]struct{})

	for rows.Next() {
		var fileName string
		var version int32
		var hashIdx int
		var hashVal string
		if err := rows.Scan(&fileName, &version, &hashIdx, &hashVal); err != nil {
			log.Println(err)
			return nil, err
		}
		allFileName[fileName] = struct{}{}
	}

	// Prepare the SQL statement outside of the loop
	stmt, err = db.Prepare(getDistinctFileName)
	if err != nil {
		log.Println("Error During Meta Load")
		return nil, err
	}
	defer stmt.Close()

	for key := range allFileName {
		rows, err := stmt.Query(key)
		if err != nil {
			fmt.Println("Error get fileName key")
		}
		cur := &FileMetaData{}
		var curHashList []string
		for rows.Next() {
			var fileName string
			var version int32
			var hashIdx int
			var hashVal string
			rows.Scan(&fileName, &version, &hashIdx, &hashVal)

			cur.Filename = fileName
			cur.Version = version
			curHashList = append(curHashList, hashVal)
		}
		cur.BlockHashList = curHashList
		fileMetaMap[cur.Filename] = cur
	}

	PrintMetaMap(fileMetaMap)

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
