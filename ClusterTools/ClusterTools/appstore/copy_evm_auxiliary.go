package appstore

import (
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
	bfPrefixStart     = "bloomFilter"
	bfPrefixEnd       = "bloomFiltes"
	txHashPrefixStart = "txHash"
	txHashPrefixEnd   = "txHasi"

	newBfPrefix = "bf"
	newThPrefix = "th"
)

func CopyEvmAuxiliary(srcDBPath, destDBPath string, batchSize, logLevel uint64, bloomFilter, txHash bool) error {
	dbName := strings.TrimSuffix(path.Base(srcDBPath), ".db")
	dbDir := path.Dir(srcDBPath)
	appDb, err := db.NewGoLevelDBWithOpts(dbName, dbDir, &opt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", srcDBPath)
	}
	defer appDb.Close()
	tree := iavl.NewMutableTree(appDb, 0)
	if _, err := tree.Load(); err != nil {
		return errors.Wrap(err, "cannot load appdb tree")
	}

	destDB, err := leveldb.OpenFile(destDBPath, nil)
	if err != nil {
		return errors.Wrap(err, "opening target database")
	}
	defer destDB.Close()

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

	if bloomFilter {
		tree.IterateRange(
			[]byte(bfPrefixStart),
			[]byte(bfPrefixEnd),
			true,
			func(key, value []byte) bool {
				if !hasPrefix(key, []byte(bfPrefixStart)) {
					log.Printf("key does not have prefix, skipped %s\n", string(key))
					return false
				}

				numKeys++
				if progressInterval > 0 && numKeys%progressInterval == 0 {
					log.Println(numKeys, "keys processed: current key", string(key))
				}

				key, err := formatPrefixes(key, []byte(bfPrefixStart), []byte(newBfPrefix))
				if err != nil {
					log.Printf("failed to format prefixes of %s\n", string(key))
					return false
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
		if itError != nil {
			return *itError
		}
		if err := destDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
			return errors.Wrapf(err, "write batch after %v keys", numKeys)
		}
		log.Println("finished extracting", string(bfPrefixStart))
	}
	if txHash {
		tree.IterateRange(
			[]byte(txHashPrefixStart),
			[]byte(txHashPrefixEnd),
			true,
			func(key, value []byte) bool {
				if !hasPrefix(key, []byte(txHashPrefixStart)) {
					log.Printf("key does not have prefix, skipped %s\n", string(key))
					return false
				}

				numKeys++
				if progressInterval > 0 && numKeys%progressInterval == 0 {
					log.Println(numKeys, "keys processed: current key", string(key))
				}

				key, err := formatPrefixes(key, []byte(txHashPrefixStart), []byte(newThPrefix))
				if err != nil {
					log.Printf("failed to format prefixes of %s\n", string(key))
					return false
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
		if itError != nil {
			return *itError
		}
		if err := destDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
			return errors.Wrapf(err, "write batch after %v keys", numKeys)
		}
		log.Println("finished extracting", string(txHashPrefixStart))
	}

	if err != nil {
		log.Println("failed to close destination db", "err", err)
	}

	now := time.Now()
	elapsed := now.Sub(startTime).Seconds()
	log.Printf("copy succesful, time taken %v seconds, %v keys copied\n", elapsed, numKeys)

	return nil
}
