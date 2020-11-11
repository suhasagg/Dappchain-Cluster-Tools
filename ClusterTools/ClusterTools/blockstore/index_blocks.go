package blockstore

import (
	"encoding/binary"
	"log"
	"math"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/tendermint/blockchain"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/node"
)

func hashKey(hash []byte) []byte {
	return append([]byte("BH:"), hash...)
}

// IndexBlockStore indexes the blocks in the source DB by hash and then writes the index out to the
// destination DB.
func IndexBlockStore(rootPath, destDBPath string, batchSize, logLevel int64) error {
	cfg, err := parseConfig(rootPath)
	if err != nil {
		return err
	}
	dbProvider := node.DefaultDBProvider
	blockStoreDB, err := dbProvider(&node.DBContext{"blockstore", cfg})
	if err != nil {
		return err
	}
	defer blockStoreDB.Close()
	blockStore := blockchain.NewBlockStore(blockStoreDB)

	dbName := strings.TrimSuffix(path.Base(destDBPath), ".db")
	dbDir := path.Dir(destDBPath)
	destDB, err := leveldb.OpenFile(filepath.Join(dbDir, dbName+".db"), nil)
	if err != nil {
		return errors.Wrap(err, "failed to open destination DB")
	}
	defer destDB.Close()
	batch := new(leveldb.Batch)

	progressInterval := uint64(0)
	if logLevel > 0 {
		progressInterval = uint64(blockStore.Height()) / uint64(math.Pow(10, float64(logLevel)))
	}
	for height := uint64(1); height < uint64(blockStore.Height()); height++ {
		blockmeta := blockStore.LoadBlockMeta(int64(height))
		heightBuffer := make([]byte, 8)
		binary.BigEndian.PutUint64(heightBuffer, height)
		if blockmeta == nil {
			log.Printf("blockmeta is nil at height %d", height)
			continue
		}
		batch.Put(hashKey(blockmeta.BlockID.Hash), heightBuffer)

		if (progressInterval > 0) && (height%progressInterval == 0) {
			log.Printf("%v blocks processed: %v%% done", height, (100*height)/uint64(blockStore.Height()))
		}

		if height%uint64(batchSize) == 0 {
			if err := destDB.Write(batch, &opt.WriteOptions{Sync: false}); err != nil {
				return errors.Wrap(err, "failed to write batch to DB")
			}
			batch.Reset()
		}
	}
	if err := destDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return errors.Wrap(err, "failed to write batch to DB")
	}

	return nil
}

func parseConfig(rootPath string) (*config.Config, error) {
	v := viper.New()
	v.AutomaticEnv()

	v.SetEnvPrefix("TM")
	v.SetConfigName("config")             // name of config file (without extension)
	v.AddConfigPath(rootPath + "/config") // search root directory
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	conf := config.DefaultConfig()
	err = v.Unmarshal(conf)
	if err != nil {
		return nil, err
	}
	conf.SetRoot(rootPath)
	conf.Mempool.WalPath = "data/mempool.wal"

	config.EnsureRoot(rootPath)
	return conf, err
}
