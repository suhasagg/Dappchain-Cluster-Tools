package appstore

import (
	"encoding/binary"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/iavl"
	"github.com/tendermint/tendermint/libs/db"
)

var (
	valueDBHeaderPrefix = []byte("dbh")
	valueDBVersionKey   = PrefixKey(valueDBHeaderPrefix, []byte("v"))
)

// TODO: import this from go-loom
func PrefixKey(keys ...[]byte) []byte {
	size := len(keys) - 1
	for _, key := range keys {
		size += len(key)
	}
	buf := make([]byte, 0, size)

	for i, key := range keys {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = append(buf, key...)
	}
	return buf
}

func ExtractIAVLTreeValuesFromDB(srcDBPath, destDBPath string, treeVersion, logLevel, batchSize int64) error {
	dbName := strings.TrimSuffix(path.Base(srcDBPath), ".db")
	dbDir := path.Dir(srcDBPath)
	appDB, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", srcDBPath)
	}
	defer appDB.Close()

	mutableTree := iavl.NewMutableTree(appDB, 0)
	if treeVersion == 0 {
		treeVersion, err = mutableTree.Load()
		if err != nil {
			return errors.Wrap(err, "failed to load mutable tree")
		}
	}
	immutableTree, err := mutableTree.GetImmutable(treeVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to load immutable tree for version %v", treeVersion)
	}

	// TM LevelDB wrapper adds .db suffix, so gotta remove it to prevent duplication
	dbName = strings.TrimSuffix(path.Base(destDBPath), ".db")
	dbDir = path.Dir(destDBPath)
	destDB, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", destDBPath)
	}
	defer destDB.Close()

	keyCount := uint64(0)
	leaves := uint(immutableTree.Size())
	var progressInterval uint64
	if logLevel > 0 {
		progressInterval = uint64(leaves / uint(math.Pow(10, float64(logLevel))))
	}

	fmt.Printf("IAVL tree height %v with %v keys\n", immutableTree.Height(), immutableTree.Size())

	startTime := time.Now()
	batch := new(leveldb.Batch)
	immutableTree.Iterate(func(key, value []byte) bool {
		batch.Put(key, value)
		if batch.Len() > int(batchSize) {
			if err := destDB.DB().Write(batch, &opt.WriteOptions{Sync: false}); err != nil {
				panic(err)
			}
			batch.Reset()
		}

		keyCount++
		if progressInterval > 0 && (keyCount%progressInterval) == 0 {
			elapsed := time.Since(startTime).Minutes()
			fractionDone := float64(keyCount) / float64(leaves)
			expected := elapsed / fractionDone

			fmt.Printf(
				"%v%% done in %v mins. ETA %v mins.\n",
				int(fractionDone*100), int(elapsed), int(expected-elapsed),
			)
		}
		return false
	})

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(treeVersion))
	batch.Put(valueDBVersionKey, buf)

	return destDB.DB().Write(batch, &opt.WriteOptions{Sync: true})
}
