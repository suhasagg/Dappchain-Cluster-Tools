package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/dappchain/clusterkit/blockstore"
)

func newIndexBlockStoreCommand() *cobra.Command {
	var batchSize, logLevel int64
	cmd := &cobra.Command{
		Use:   "index-by-hash <path/to/src/chaindata> <path/to/dest/db>",
		Short: "Indexes an existing block store by hash and writes the index to a new DB.",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			srcDBPath, err := filepath.Abs(args[0])
			if err != nil {
				return fmt.Errorf("Failed to resolve source DB path '%s'", args[0])
			}
			destDBPath, err := filepath.Abs(args[1])
			if err != nil {
				return fmt.Errorf("Failed to resolve destination DB path '%s'", args[1])
			}

			if info, err := os.Stat(srcDBPath); os.IsNotExist(err) || !info.IsDir() {
				return fmt.Errorf("DB cannot be found at '%s'", srcDBPath)
			}
			if _, err := os.Stat(destDBPath); !os.IsNotExist(err) {
				return fmt.Errorf("Something already exists at '%s', please specify another path", destDBPath)
			}
			start := time.Now()
			err = blockstore.IndexBlockStore(srcDBPath, destDBPath, batchSize, logLevel)
			if err != nil {
				fmt.Printf("Failed to extract keys & values, time taken: %v mins\n", time.Now().Sub(start).Minutes())
				return err
			}
			fmt.Printf("Extracted keys & values, time taken: %v mins\n", time.Now().Sub(start).Minutes())
			return nil
		},
	}
	cmd.Flags().Int64Var(&batchSize, "batch-size", 10000, "Number of keys to write in each batch.")
	cmd.Flags().Int64Var(&logLevel, "log", 0, "How often progress output should be printed. 1 - every 10%, 2 - every 1%, 3 - every 0.1%.")
	return cmd
}

func newRollbackBlockStoreCommand() *cobra.Command {
	var height int64
	cmd := &cobra.Command{
		Use:   "rollback <path/to/chaindata> --height <block-height>",
		Short: "Rolls back the blockstore.db to the specified height.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			blockStore := blockstore.NewBlockStore(args[0], false)
			defer blockStore.Close()

			if err := blockStore.Rollback(height, nil); err != nil {
				return err
			}
			fmt.Printf("Rolled back blockstore.db to height %d\n", height)
			return nil
		},
	}

	cmd.Flags().Int64Var(&height, "height", 1, "Block height to rollback to.")
	return cmd
}

func newPurgeBlockStoreCommand() *cobra.Command {
	var batchSize, logLevel, height int64
	var skipMissingBlock, skipCompaction bool
	cmd := &cobra.Command{
		Use:   "purge <path/to/chaindata> --height <block-height>",
		Short: "Remove blocks in the blockstore.db below the specified height.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if info, err := os.Stat(args[0]); os.IsNotExist(err) || !info.IsDir() {
				return fmt.Errorf("chaindata cannot be found at '%s'", args[0])
			}

			blockStore := blockstore.NewBlockStore(args[0], false)
			defer blockStore.Close()

			if err := blockStore.Purge(height, nil, batchSize, logLevel, skipMissingBlock, skipCompaction); err != nil {
				return err
			}
			fmt.Printf("Purge blockstore.db below height %d\n", height)
			return nil
		},
	}

	cmd.Flags().Int64Var(&height, "height", 1, "Block height to .")
	cmd.Flags().Int64Var(&batchSize, "batch-size", 10000, "Number of blocks to write in each batch.")
	cmd.Flags().Int64Var(&logLevel, "log", 0, "How often progress output should be printed. 1 - every 10%, 2 - every 1%, 3 - every 0.1%.")
	cmd.Flags().BoolVar(&skipMissingBlock, "skip-missing", false, "Skip the missing blocks during purging")
	cmd.Flags().BoolVar(&skipCompaction, "skip-compaction", false, "Don't compact DB after purging")
	return cmd
}

func newBlockStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "block-store",
		Short: "Tools that operate on the block store (chaindata/data/blockstore.db)",
	}
	cmd.AddCommand(
		newIndexBlockStoreCommand(),
		newRollbackBlockStoreCommand(),
		newPurgeBlockStoreCommand(),
	)
	return cmd
}
