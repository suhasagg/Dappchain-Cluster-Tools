package blockstore

import (
	"fmt"
	"log"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tendermint/tendermint/blockchain"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/types"
)

var (
	calcBlockMetaPrefix = []byte("H:")
)

type BlockStore struct {
	blockStoreDB dbm.DB
	*blockchain.BlockStore
	chainDataDir string
}

func NewBlockStore(chainDataDir string, readOnly bool) *BlockStore {
	var blockStoreDB dbm.DB

	if readOnly {
		var err error
		blockStoreDB, err = dbm.NewGoLevelDBWithOpts(
			"blockstore", path.Join(chainDataDir, "data"),
			&opt.Options{
				ReadOnly: true,
			},
		)
		if err != nil {
			panic("failed to load block store")
		}
	} else {
		blockStoreDB = dbm.NewDB("blockstore", "leveldb", path.Join(chainDataDir, "data"))
	}

	return &BlockStore{
		blockStoreDB: blockStoreDB,
		BlockStore:   blockchain.NewBlockStore(blockStoreDB),
		chainDataDir: chainDataDir,
	}
}

func (bs *BlockStore) Close() {
	bs.blockStoreDB.Close()
}

func (bs *BlockStore) Has(key []byte) bool {
	return bs.blockStoreDB.Has(key)
}

func (bs *BlockStore) Height() int64 {
	return blockchain.LoadBlockStoreStateJSON(bs.blockStoreDB).Height
}

func (bs *BlockStore) LoadBlock(height int64) *types.Block {
	return bs.BlockStore.LoadBlock(height)
}

func (bs *BlockStore) LoadSeenCommit(height int64) *types.Commit {
	return bs.BlockStore.LoadSeenCommit(height)
}

func (bs *BlockStore) SaveSeenCommit(blockHeight int64, newSeenCommit *types.Commit) error {
	binary, err := cdc.MarshalBinaryBare(newSeenCommit)
	if err != nil {
		return err
	}

	bs.blockStoreDB.Set(calcSeenCommitKey(blockHeight), binary)
	return nil
}

// Rollback removes any blocks in the block store with a height higher than the target height.
// If the optional tx index store is passed in then the tx results from the blocks that are removed
// from the block store will be removed from the tx index store.
func (bs *BlockStore) Rollback(targetHeight int64, txIndexStore *TxIndexStore) error {
	latestHeight := bs.Height()

	if targetHeight >= latestHeight {
		return fmt.Errorf(
			"can't rollback the block store to block %d, current height is %d",
			targetHeight, latestHeight,
		)
	}

	txs := []types.Tx{}
	batch := bs.blockStoreDB.NewBatch()
	for height := latestHeight; height > targetHeight; height-- {
		meta := bs.LoadBlockMeta(height)

		if txIndexStore != nil {
			block := bs.LoadBlock(height)
			txs = append(txs, block.Data.Txs...)
		}

		batch.Delete(calcBlockMetaKey(height))
		for i := 0; i < meta.BlockID.PartsHeader.Total; i++ {
			batch.Delete(calcBlockPartKey(height, i))
		}
		batch.Delete(calcBlockCommitKey(height - 1))
		batch.Delete(calcSeenCommitKey(height))
	}
	batch.WriteSync()
	blockchain.BlockStoreStateJSON{Height: targetHeight}.Save(bs.blockStoreDB)

	// TODO: Needs testing, curently complete untested.
	if txIndexStore != nil {
		if err := txIndexStore.Delete(txs); err != nil {
			return err
		}
	}
	return nil
}

// Purge removes any blocks in the block store below the target height.
// If the optional tx index store is passed in then the tx results from the blocks that are removed
// from the block store will be removed from the tx index store.
func (bs *BlockStore) Purge(targetHeight int64, txIndexStore *TxIndexStore, batchSize, logLevel int64, skipMissing, skipCompaction bool) error {
	latestHeight := bs.Height()

	if targetHeight > latestHeight {
		return fmt.Errorf(
			"can't purge the block store below block %d, current height is %d",
			targetHeight, latestHeight,
		)
	}

	var progressInterval int64
	if logLevel > 0 {
		progressInterval = int64(targetHeight / int64(math.Pow(10, float64(logLevel))))
	}

	oldestHeight := int64(-1)
	// find the oldest block
	it := bs.blockStoreDB.Iterator(calcBlockMetaPrefix, prefixRangeEnd(calcBlockMetaPrefix))
	for ; it.Valid(); it.Next() {
		oldestBlockMetaKey := it.Key()
		oldestHeight = getHeightFromKey(oldestBlockMetaKey)
		break
	}

	if oldestHeight >= targetHeight {
		return fmt.Errorf("no block below block %d", targetHeight)
	}
	log.Println("oldest block height", oldestHeight)

	txs := []types.Tx{}
	batch := bs.blockStoreDB.NewBatch()
	numHeight := int64(0)
	for height := targetHeight - 1; height >= oldestHeight; height-- {
		// if block metadata is not found, stop purging
		if !bs.Has(calcBlockMetaKey(height)) {
			log.Printf("block is missing at %d height", height)
			if skipMissing {
				continue
			}
			break
		}

		meta := bs.LoadBlockMeta(height)
		if txIndexStore != nil {
			block := bs.LoadBlock(height)
			txs = append(txs, block.Data.Txs...)
		}

		batch.Delete(calcBlockMetaKey(height))
		for i := 0; i < meta.BlockID.PartsHeader.Total; i++ {
			batch.Delete(calcBlockPartKey(height, i))
		}
		batch.Delete(calcBlockCommitKey(height - 1))
		batch.Delete(calcSeenCommitKey(height))

		if progressInterval > 0 && numHeight%progressInterval == 0 {
			log.Println(numHeight, "blocks processed: current height", height)
		}

		if numHeight%batchSize == 0 {
			batch.Write()
			batch = bs.blockStoreDB.NewBatch()
		}
		numHeight++
	}
	batch.WriteSync()

	if !skipCompaction {
		bs.blockStoreDB.Close()
		db, err := leveldb.OpenFile(path.Join(bs.chainDataDir, "data", "blockstore.db"), nil)
		if err != nil {
			return fmt.Errorf("cannot open blockstore.db for compaction, %s", err.Error())
		}
		defer db.Close()
		if err := db.CompactRange(util.Range{}); err != nil {
			return fmt.Errorf("failed to compact db, %s", err.Error())
		}
		log.Println("finished DB compaction")
	}

	// TODO: Needs testing, curently complete untested.
	if txIndexStore != nil {
		if err := txIndexStore.Delete(txs); err != nil {
			return err
		}
	}
	return nil
}

func getHeightFromKey(key []byte) int64 {
	val := strings.Split(string(key), ":")
	if len(val) > 1 {
		height, err := strconv.ParseInt(val[1], 10, 64)
		if err != nil {
			return 0
		}
		return height
	}
	return 0
}

func calcBlockMetaKey(height int64) []byte {
	return []byte(fmt.Sprintf("H:%v", height))
}

func calcBlockPartKey(height int64, partIndex int) []byte {
	return []byte(fmt.Sprintf("P:%v:%v", height, partIndex))
}

func calcBlockCommitKey(height int64) []byte {
	return []byte(fmt.Sprintf("C:%v", height))
}

func calcSeenCommitKey(height int64) []byte {
	return []byte(fmt.Sprintf("SC:%v", height))
}
