package appstore

import (
	"fmt"
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

// CloneIAVLTreeFromDB copies the IAVL tree matching the specified height to a new DB.
// The srcValueDBPath parameter may be empty, otherwise it should be the path to app_state.db.
func CloneIAVLTreeFromDB(
	srcDBPath, srcValueDBPath, destDBPath string, height int64, logLevel, savesPerCommit uint64,
) error {
	dbName := strings.TrimSuffix(path.Base(srcDBPath), ".db")
	dbDir := path.Dir(srcDBPath)
	appDb, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", srcDBPath)
	}
	defer appDb.Close()

	dbName = strings.TrimSuffix(path.Base(destDBPath), ".db")
	dbDir = path.Dir(destDBPath)
	newAppDb, err := db.NewGoLevelDB(dbName, dbDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %v", destDBPath)
	}
	defer newAppDb.Close()

	var tree *iavl.MutableTree
	if len(srcValueDBPath) == 0 {
		tree = iavl.NewMutableTree(appDb, 0)
		if _, err := tree.LoadVersion(height); err != nil {
			return errors.Wrapf(err, "failed to load IAVL tree version %v", height)
		}
	} else {
		dbName := strings.TrimSuffix(path.Base(srcValueDBPath), ".db")
		dbDir := path.Dir(srcValueDBPath)
		valueDB, err := db.NewGoLevelDB(dbName, dbDir)
		if err != nil {
			return errors.Wrapf(err, "failed to open %v", srcValueDBPath)
		}
		defer valueDB.Close()

		appNodeDB := iavl.NewNodeDB(appDb, 10000, valueDB.Get)
		tree = iavl.NewMutableTreeWithNodeDB(appNodeDB)
		lastVer, err := tree.LoadVersion(height)
		if err != nil {
			return errors.Wrapf(err, "failed to load IAVL tree version %v", height)
		}
		// If app_state.db is being used we can't load any arbitrary height, the app_state.db only
		// has data for the latest height
		if (height > 0) && (lastVer != height) {
			return fmt.Errorf("height %d doesn't match latest IAVL tree version %d", height, lastVer)
		}
	}

	newNdb := iavl.NewNodeDB(newAppDb, 10000, nil)

	if logLevel < 1 {
		if _, _, err := tree.SaveVersionToDB(height, newNdb, savesPerCommit, nil); err != nil {
			return errors.Wrapf(err, "failed to save version %v to db", height)
		}
	} else {
		leafCount := uint64(0)
		leaves := uint(tree.Size())
		debugPeriod := uint64(leaves / uint(math.Pow(10, float64(logLevel))))
		if debugPeriod == 0 {
			debugPeriod = 10
		}
		log.Printf("IAVL tree height %v with %v keys", tree.Height(), tree.Size())
		startTime := time.Now()
		lastVisted := time.Now()
		// TODO: don't think this works correclty if version isn't latest, and even then
		//       SaveVersionToDBDebug() needs a bit of cleanup
		if _, _, err := tree.SaveVersionToDB(
			height,
			newNdb,
			savesPerCommit,
			func(height int8) bool {
				if height != 0 {
					return false
				}
				leafCount++
				if leafCount%debugPeriod != 0 {
					return false
				}
				now := time.Now()
				elapsed := now.Sub(startTime).Seconds()
				fractionDone := float64(leafCount) / float64(leaves)
				expected := elapsed / fractionDone

				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				log.Printf(
					"%v leaf nodes loaded %v%% of total. Time taken so far %v seconds %v seconds since last log. Expected to complete in %v seconds. Memroy used %vMb.",
					leafCount,
					uint(fractionDone*100),
					elapsed,
					now.Sub(lastVisted).Seconds(),
					expected-elapsed,
					memStats.Alloc/1000000,
				)
				lastVisted = now
				return false
			},
		); err != nil {
			return errors.Wrapf(err, "failed to save IAVL tree version %v", height)
		}
		now := time.Now()
		elapsed := now.Sub(startTime)
		log.Printf("Finished reeading in database, time taken %v seconds", elapsed)
	}

	return nil
}
