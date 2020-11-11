package appstore

import (
	"bytes"
	"log"
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

const (
	prefixStart   = "vm"
	prefixEnd     = "vn"
	rootKey       = "vmroot"
	evmRootPrefix = "evmroot"
)

var (
	defaultRoot = []byte{1}
)

func CopyEvmToLevelDb(srcDBPath, destDBPath string, batchSize, logLevel uint64, height int64) error {
	dbName := strings.TrimSuffix(path.Base(srcDBPath), ".db")
	dbDir := path.Dir(srcDBPath)
	appDb, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", srcDBPath)
	}
	tree := iavl.NewMutableTree(appDb, 0)
	if _, err := tree.LoadVersion(height); err != nil {
		return errors.Wrap(err, "cannot load appdb tree")
	}
	appVersion := tree.Version()
	log.Printf("extract EVM state at height %d", appVersion)

	destDB, err := leveldb.OpenFile(destDBPath, nil)
	if err != nil {
		return errors.Wrap(err, "opening target database")
	}

	leaves := uint(tree.Size())
	log.Printf("Source app.db size %v data values", leaves)

	startTime := time.Now()
	batch := new(leveldb.Batch)
	numKeys := uint64(0)
	progressInterval := uint64(0)
	if logLevel > 0 {
		progressInterval = uint64(leaves / uint(math.Pow(10, float64(logLevel))))
	}
	var itError *error
	tree.IterateRange(
		[]byte(prefixStart),
		[]byte(prefixEnd),
		true,
		func(key, value []byte) bool {
			if !hasPrefix(key, []byte(prefixStart)) {
				log.Printf("key does not have prefix, skipped %s\n", string(key))
				return false
			}

			numKeys++
			if progressInterval > 0 && numKeys%progressInterval == 0 {
				log.Println(numKeys, "keys processed: current key", string(key))
			}

			if bytes.Equal(prefixKey([]byte(prefixStart), []byte(rootKey)), key) {
				log.Printf("Copy vmvmroot from app.db to vmevmroot of evm.db at height %d\n", appVersion)
				// if Patricia root is nil, set it to defaultRoot for EvmStore
				if value == nil {
					value = defaultRoot
				}
				batch.Put(evmRootKey(appVersion), value)
			}
			batch.Put(key, value)
			if batch.Len() > int(batchSize) {
				if err := destDB.Write(batch, &opt.WriteOptions{Sync: false}); err != nil {
					*itError = errors.Wrapf(err, "write batch after %v keys", numKeys)
					return true
				}
				batch.Reset()
			}
			return false
		},
	)

	if numKeys == 0 {
		log.Printf("EVM state is empty, put default evmroot key at height %d\n", appVersion)
		batch.Put(evmRootKey(appVersion), defaultRoot)
	}

	if itError != nil {
		return *itError
	}

	appDb.Close()
	if err := destDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return errors.Wrapf(err, "write batch after %v keys", numKeys)
	}
	err = destDB.Close()
	if err != nil {
		log.Println("failed to close destination db", "err", err)
	}

	now := time.Now()
	elapsed := now.Sub(startTime).Seconds()
	log.Printf("copy succesful, time taken %v seconds, %v keys copied\n", elapsed, numKeys)

	return nil
}
