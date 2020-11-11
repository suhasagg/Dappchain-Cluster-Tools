package appstore

import (
	"log"
	"math"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/tendermint/iavl"
	"github.com/tendermint/tendermint/libs/db"
)

type IAVLStoreStats struct {
	NumKeys         uint64
	TotalKeyBytes   uint64
	TotalValueBytes uint64
	TimeTaken       time.Duration
}

func TotalData(dbPath, prefix string, blockNumber int64, logLevel uint64) (IAVLStoreStats, error) {
	dbName := strings.TrimSuffix(path.Base(dbPath), ".db")
	dbDir := path.Dir(dbPath)
	appDb, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return IAVLStoreStats{}, errors.Wrapf(err, "failed to open %v", dbPath)
	}

	tree := iavl.NewMutableTree(appDb, 0)
	_, err = tree.LoadVersion(blockNumber)
	if err != nil {
		return IAVLStoreStats{}, err
	}

	start := []byte(nil)
	if len(prefix) > 0 {
		start = []byte(prefix)
	}
	end := prefixRangeEnd(start)

	numKeys := uint64(0)
	keyTotal := uint64(0)
	valueTotal := uint64(0)
	size := uint(tree.Size())
	debugPeriod := uint64(size / uint(math.Pow(10, float64(logLevel))))
	if debugPeriod == 0 {
		debugPeriod = 10
	}
	log.Printf("Database of height %v with %v keys", tree.Height(), tree.Size())

	startTime := time.Now()
	tree.IterateRangeInclusive(
		start,
		end,
		true,
		func(key, value []byte, version int64) bool {
			numKeys++
			keyTotal += uint64(len(key))
			valueTotal += uint64(len(value))
			if logLevel > 0 && numKeys%debugPeriod == 0 {
				now := time.Now()
				elapsed := now.Sub(startTime).Seconds()
				fractionDone := float64(numKeys) / float64(size)
				expected := elapsed / fractionDone
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				log.Printf(
					"%v Keys, %v%% of the total. Total memory of kv pairs read so far is %v bytes. Time taken so far is %v, expected to complete in %v seconds. Memroy used %vMb Sys %vMb, HeapIdle %vMb.",
					numKeys,
					uint(fractionDone*100),
					keyTotal+valueTotal,
					elapsed,
					expected-elapsed,
					memStats.Alloc/1000000,
					memStats.Sys/1000000,
					memStats.HeapIdle/1000000,
				)
			}

			return false
		},
	)

	return IAVLStoreStats{
		NumKeys:         numKeys,
		TotalKeyBytes:   keyTotal,
		TotalValueBytes: valueTotal,
		TimeTaken:       time.Now().Sub(startTime),
	}, nil
}

// todo export form loomchian/store
// Returns the bytes that mark the end of the key range for the given prefix.
func prefixRangeEnd(prefix []byte) []byte {
	if prefix == nil {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for {
		if end[len(end)-1] != byte(255) {
			end[len(end)-1]++
			break
		} else if len(end) == 1 {
			end = nil
			break
		}
		end = end[:len(end)-1]
	}
	return end
}
