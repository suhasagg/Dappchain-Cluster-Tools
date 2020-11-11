package blockstore

import (
	"fmt"
	"path"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/tendermint/blockchain"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/types"
)

type TxIndexStore struct {
	txIndexDB dbm.DB
	*blockchain.BlockStore
}

func NewTxIndexStore(chainDataDir string, readOnly bool) *TxIndexStore {
	var txIndexDB dbm.DB

	if readOnly {
		var err error
		txIndexDB, err = dbm.NewGoLevelDBWithOpts(
			"tx_index", path.Join(chainDataDir, "data"),
			&opt.Options{
				ReadOnly: true,
			},
		)
		if err != nil {
			panic("failed to load tx index store")
		}
	} else {
		txIndexDB = dbm.NewDB("tx_index", "leveldb", path.Join(chainDataDir, "data"))
	}

	return &TxIndexStore{
		txIndexDB: txIndexDB,
	}
}

func (s *TxIndexStore) Close() {
	s.txIndexDB.Close()
}

func (s *TxIndexStore) Delete(txs []types.Tx) error {
	batch := s.txIndexDB.NewBatch()
	for _, tx := range txs {
		rawTxResult := s.txIndexDB.Get(tx.Hash())
		if len(rawTxResult) > 0 {
			batch.Delete(tx.Hash())

			txResult := types.TxResult{}
			if err := cdc.UnmarshalBinaryBare(rawTxResult, &txResult); err != nil {
				return fmt.Errorf("error unmarshaling TxResult: %v", err)
			}
			batch.Delete(txIndexHeightKey(&txResult))
		}
	}
	batch.Write()
	return nil
}

func txIndexHeightKey(result *types.TxResult) []byte {
	return []byte(fmt.Sprintf("%s/%d/%d/%d",
		types.TxHeightKey,
		result.Height,
		result.Height,
		result.Index,
	))
}
