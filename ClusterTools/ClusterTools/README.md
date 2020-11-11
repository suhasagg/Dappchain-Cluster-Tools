# clusterkit
Maintenance tools for  DAppChain clusters

1)
## Shrink app.db (by cloning latest IAVL store version to a new DB)

Each `DAppchain` node has an `app.db`, which is a LevelDB database that stores the persistent app state.
Every time a new block is executed by a node the resulting state changes are persisted to `app.db`,
previous state is pretty much never removed so `app.db` growth is unbounded. The `app-store clone` command provides a way to clone the latest persisted app state to a new `app.db`, while ignoring all
the historical app state. Since historical app state is not copied to the `app.db` clone it's
impossible to rollback the clone to a previous state.

Ensure that the node the `app.db` is being cloned from is not running, then execute the following
command:
```bash
clusterkit app-store clone <path/to/src/app.db> <path/to/dest/app.db> --log 1 --saves-per-commit 10000
```

`path/to/dest/app.db` can then be swapped in instead of `path/to/src/app.db` on the source node,
or used to spin up another node.

2)
## Prune blockstore.db

Tendermint stores block data in `blockstore.db`, this contains data for each block since genesis
and since Tendermint never deletes old data the growth of this DB is unbounded. The block data is
necessary to replay the chain from genesis, but at a certain point it becomes impractical to do so
due to the time requirements. It's also not necessary to have every node in the cluster with the
full `blockstore.db` as long as a jump-start archive is provided for spinning up new nodes. A full
backup of `blockstore.db` should be maintained, either offline or on archival nodes (which should
be non-validators with the sole purpose of storing blocks).

To remove old blocks from the `blockstore.db` first stop the node, then execute the following command:
```bash
clusterkit block-store purge <path/to/chaindata> --height <oldest-height-to-keep> --log 1
```

The `height` flag is used to specify the height of the oldest block to keep in the DB, any blocks
with a lower height will be deleted from the DB.

3)
## Extract EVM state from app.db to a new DB

The `app-store extract-evm-state` command will copy all the `vm`-prefixed keys from the IAVL store
persisted to `app.db` to a new LevelDB.

```bash
clusterkit app-store extract-evm-state <path/to/src/app.db> <path/to/dest/evm.db> --log 1 --batch-size 10000
```

4)
## Index the block store by block hash
Tendermint doesn't index blocks by hash, only by height, this makes it difficult to look up blocks
by hash in a reasonable amount of time. `DAppchain` can use a `block_index.db` to speedup lookups
by block hash, to bootstrap this DB on an existing node use the following command. 
```bash
clusterkit block-store index-by-hash <path/to/src/chaindata> <path/to/dest/db> --log 1 --batch-size 10000
```


