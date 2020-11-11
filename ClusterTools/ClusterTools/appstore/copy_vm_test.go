package appstore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tendermint/iavl"
	"github.com/tendermint/tendermint/libs/db"
)

func TestCopyEvmToLevelDb(t *testing.T) {
	_ = os.RemoveAll("./tempApp.db")
	_, err := os.Stat("./tempApp.db")
	require.True(t, os.IsNotExist(err))

	_ = os.RemoveAll("./tempEvm.db")
	_, err = os.Stat("./tempEvm.db")
	require.True(t, os.IsNotExist(err))

	type record struct {
		key   string
		value string
	}
	records := []record{
		{"before", "aa"},
		{"vl", "bb"},
		{"vm", "cc"},
		{"vm\x00", "dd"},
		{"vmq", "dd"},
		{"vm\x001", "ee"},
		{"vm\x00ffff", "ff"},
		{"vm_ffff", "ff"},
		{"vm\x00ggg", "gg"},
		{"vm\x00hhh", "hh"},
		{"vn", "ii"},
		{"vn_", "kk"},
		{"vn_2", "ll"},
		{"vn\x002", "ll"},
		{"z_after", "jj"},
	}

	tempSourceDB, err := db.NewGoLevelDB("tempApp", ".")
	require.NoError(t, err)
	tree := iavl.NewMutableTree(tempSourceDB, 0)
	_, err = tree.Load()
	require.NoError(t, err)
	for _, r := range records {
		tree.Set([]byte(r.key), []byte(r.value))
	}
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)
	tempSourceDB.Close()

	require.NoError(t, CopyEvmToLevelDb("./tempApp.db", "./tempEvm.db", 2, 0, 0))

	destDB, err := leveldb.OpenFile("./tempEvm.db", nil)
	require.NoError(t, err)
	defer destDB.Close()
	for _, r := range records {
		if len(r.key) > 3 {
			has, err := destDB.Has([]byte(r.key), nil)
			require.NoError(t, err)
			require.Equal(t, r.key[:3] == "vm\x00", has)
		}
	}

	iter := destDB.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		found := false
		for _, r := range records {
			if string(iter.Key()) == r.key {
				found = true
				break
			}
		}
		require.True(t, found)
	}
}
