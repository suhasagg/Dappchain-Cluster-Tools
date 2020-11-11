package blockstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/blockchain"
	"github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/node"
	"github.com/tendermint/tendermint/types"
)

const (
	rootPath     = "./test_data"
	blockIndexDb = "./test_data/blockIndex.db"
)

var (
	cdc = amino.NewCodec()

	blockStoreKey = []byte("blockStore")
	tests         = []struct {
		height uint64
		hash   []byte
	}{
		{1, []byte("0881F34C2EF8B3DF638DDB35A3B1B7B4C5C50F0CB3CD4FE7E2DB97D5E3170BB0")},
		{2, []byte("11E00D8EBD28EC67442C2F3BD7A5C6270F87C10510F1062C8246345504C525D9")},
		{3, []byte("13BA75761964EB500D666564BEF80945FF12CFA9EE556942A892954AE07B596A")},
		{4, []byte("153352CA4CBC498F35C5EE10854617F64A1BEEFD75D06FC7DF1AE4F765D16EC2")},
		{5, []byte("17F4B61A5AE25586E7F51A8EAC1BBF2097D861D57AE74F76506A6DECD5C3214E")},
		{6, []byte("2F170A3E16F950518D4E8A7E3B570296C87AFBF39B092755EFF045A6827E9815")},
		{7, []byte("350C0AADE51351C11A3542BF92DBB7B2D2A5A3BE3015444F4FD8ED9293F98F59")},
		{8, []byte("35298639061A39CDC1586CFDDDBE0E44502666BE2657E9265B85E0318F967D88")},
		{9, []byte("3EA12E5325F7F47F0847A8F8B2FD05337FFF4A70977EE33D2426C9F3575313F3")},
		{10, []byte("3EC068491B3AA69EC5160C8A2FB6317124E091100704F16D45CF263EA96F9FF2")},
	}
)

func TestIndexBlockStore(t *testing.T) {
	_ = os.RemoveAll(blockIndexDb)
	_, err := os.Stat(blockIndexDb)
	require.True(t, os.IsNotExist(err))

	_ = os.RemoveAll("./test_data/data")
	_, err = os.Stat("./test_data/data")
	require.True(t, os.IsNotExist(err))

	cfg, err := parseConfig(rootPath)
	require.NoError(t, err)
	dbProvider := node.DefaultDBProvider
	blockStoreDB, err := dbProvider(&node.DBContext{"blockstore", cfg})
	require.NoError(t, err)

	for _, test := range tests {
		blockMeta := types.BlockMeta{BlockID: types.BlockID{Hash: test.hash}}
		metaBytes := cdc.MustMarshalBinaryBare(blockMeta)
		blockStoreDB.Set(calcBlockMetaKey(int64(test.height)), metaBytes)
	}

	bsj := blockchain.BlockStoreStateJSON{Height: int64(len(tests) + 1)}
	bsjBytes, err := cdc.MarshalJSON(bsj)
	require.NoError(t, err)
	blockStoreDB.SetSync(blockStoreKey, bsjBytes)
	blockStoreDB.Close()

	require.NoError(t, IndexBlockStore(rootPath, blockIndexDb, 5, 0))

	dbName := strings.TrimSuffix(path.Base(blockIndexDb), ".db")
	dbDir := path.Dir(blockIndexDb)
	blockIndexDb, err := db.NewGoLevelDB(dbName, dbDir)
	require.NoError(t, err)
	defer blockIndexDb.Close()

	for _, test := range tests {
		height := binary.BigEndian.Uint64(blockIndexDb.Get(test.hash))
		require.Equal(t, test.height, height)
	}

	iter := blockIndexDb.Iterator(nil, nil)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		found := false
		for _, test := range tests {
			if binary.BigEndian.Uint64(iter.Value()) == test.height && 0 == bytes.Compare(iter.Key(), test.hash) {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}

func calcBlockMetaKey(height int64) []byte {
	return []byte(fmt.Sprintf("H:%v", height))
}
