package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/dappchain/clusterkit/appstore"
)

func newCloneAppStoreCommand() *cobra.Command {
	var height int64
	var logLevel uint64
	var savesPerCommit uint64
	var srcValueDBPath string
	cloneAppStoreCmd := &cobra.Command{
		Use:   "clone <path/to/src/app.db> <path/to/dest/app.db>",
		Short: "Clones a single version of the IAVL tree from an IAVL store DB to a new DB",
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

			var valueDBPath string
			if len(srcValueDBPath) > 0 {
				valueDBPath, err = filepath.Abs(srcValueDBPath)
				if err != nil {
					return fmt.Errorf("Failed to resolve value DB path '%s'", srcValueDBPath)
				}
				if info, err := os.Stat(valueDBPath); os.IsNotExist(err) || !info.IsDir() {
					return fmt.Errorf("DB cannot be found at '%s'", valueDBPath)
				}
			}

			if info, err := os.Stat(srcDBPath); os.IsNotExist(err) || !info.IsDir() {
				return fmt.Errorf("DB cannot be found at '%s'", srcDBPath)
			}
			if _, err := os.Stat(destDBPath); !os.IsNotExist(err) {
				return fmt.Errorf("Something already exists at '%s', please specify another path", destDBPath)
			}

			if height > 0 {
				fmt.Println("Cloning the app store from ", srcDBPath, " at height ", height)
			} else {
				fmt.Println("Cloning the app store from ", srcDBPath, " at its current height")
			}
			start := time.Now()
			err = appstore.CloneIAVLTreeFromDB(srcDBPath, valueDBPath, destDBPath, height, logLevel, savesPerCommit)
			if err != nil {
				fmt.Println("Failed cloning ", srcDBPath, ", time taken ", time.Now().Sub(start))
				return err
			}
			fmt.Println("Finished cloning", srcDBPath, ", time taken ", time.Now().Sub(start))

			sizeOld, err := dirSize(srcDBPath)
			if err != nil {
				fmt.Printf("failed to compute size of '%s', err: %v\n", srcDBPath, err)
				return nil
			}
			fmt.Println("Original DB size ", sizeOld, " bytes")

			sizeNew, err := dirSize(destDBPath)
			if err != nil {
				fmt.Printf("failed to compute size of '%s', err: %v\n", destDBPath, err)
				return nil
			}
			fmt.Println("New DB size", sizeNew, " bytes")
			return nil
		},
	}
	cloneAppStoreCmd.Flags().Int64VarP(&height, "height", "b", 0, "block height from which to clone app store. Default is the current height.")
	cloneAppStoreCmd.Flags().Uint64VarP(&logLevel, "log", "l", 0, "log Level. Debug information displayed every (100*10^-Loglevel)% of keys. Example 1 every 10%, 2 every 1%, 3 every 0.1%")
	cloneAppStoreCmd.Flags().Uint64VarP(&savesPerCommit, "saves-per-commit", "s", 0, "Number of saves between commits. zero means no intermediate commits.")
	cloneAppStoreCmd.Flags().StringVar(&srcValueDBPath, "src-value-db", "", "Optional path to app_state.db")
	return cloneAppStoreCmd
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func newExtractEvmCommand() *cobra.Command {
	var logLevel, batchSize uint64
	var height int64
	extractEvmCommand := &cobra.Command{
		Use:   "extract-evm-state <path/to/src/app.db> <path/to/dest/db>",
		Short: "Extract the latest EVM state from app.db to a separate LevelDB",
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

			return appstore.CopyEvmToLevelDb(srcDBPath, destDBPath, batchSize, logLevel, height)
		},
	}
	extractEvmCommand.Flags().Uint64Var(&logLevel, "log", 0, "How often progress output should be printed. 1 - every 10%, 2 - every 1%, 3 - every 0.1%.")
	extractEvmCommand.Flags().Uint64Var(&batchSize, "batch-size", 10000, "Number of keys to write in each batch.")
	extractEvmCommand.Flags().Int64Var(&height, "height", 0, "app.db height at which EVM state is extracted")
	return extractEvmCommand
}

func newExtractEvmAuxCommand() *cobra.Command {
	var logLevel, batchSize uint64
	var onlyBloomFilter, onlyTxHash bool
	extractEvmCommand := &cobra.Command{
		Use:   "extract-evm-data <path/to/src/app.db> <path/to/dest/db>",
		Short: "Extract EVM bloom filter and tx hash from app.db to a separate LevelDB",
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
			bloomfilter := !onlyTxHash
			txHash := !onlyBloomFilter
			return appstore.CopyEvmAuxiliary(srcDBPath, destDBPath, batchSize, logLevel, bloomfilter, txHash)
		},
	}
	extractEvmCommand.Flags().Uint64Var(&logLevel, "log", 0, "How often progress output should be printed. 1 - every 10%, 2 - every 1%, 3 - every 0.1%.")
	extractEvmCommand.Flags().Uint64Var(&batchSize, "batch-size", 10000, "Number of keys to write in each batch.")
	extractEvmCommand.Flags().BoolVar(&onlyBloomFilter, "bloom-filters", false, "Extract bloom filters only")
	extractEvmCommand.Flags().BoolVar(&onlyTxHash, "tx-hashes", false, "Extract EVM Tx Hashes only")
	return extractEvmCommand
}

func newTotalDataCommand() *cobra.Command {
	var blockNumber int64
	var prefix string
	var logLevel uint64
	totalDataCmd := &cobra.Command{
		Use:   "total-data <path/to/app.db>",
		Short: "Displays stats for an IAVL store DB. WARNING: Might take a long time with a large DB!",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath, err := filepath.Abs(args[0])
			if err != nil {
				return fmt.Errorf("Failed to resolve source DB path '%s'", args[0])
			}
			if _, err := os.Stat(dbPath); os.IsNotExist(err) {
				return fmt.Errorf("DB not found at %s", dbPath)
			}

			stats, err := appstore.TotalData(dbPath, prefix, blockNumber, logLevel)
			if err != nil {
				return err
			}

			fmt.Printf(
				"%v keys found, key total %v bytes, values total %v bytes for a combined memory used of %v bytes.\nTime taken %v seconds.\n",
				stats.NumKeys, stats.TotalKeyBytes, stats.TotalValueBytes, stats.TotalKeyBytes+stats.TotalValueBytes, int64(stats.TimeTaken.Seconds()),
			)

			return nil
		},
	}
	totalDataCmd.Flags().StringVarP(&prefix, "prefix", "p", "", "prefix for keys to total, default \"\" to total all keys.")
	totalDataCmd.Flags().Uint64VarP(&logLevel, "log", "l", 0, "log Level. Debug information displayed every (100*10^-Loglevel)% of keys. Example 1 every 10%, 2 every 1%, 3 every 0.1%")
	totalDataCmd.Flags().Int64VarP(&blockNumber, "height", "b", 0, "block height from which to clone app store. Default is the current height.")
	return totalDataCmd
}

func newExtractValuesFromIAVLStoreCommand() *cobra.Command {
	var version, logLevel, batchSize int64
	cmd := &cobra.Command{
		Use:   "extract-values <path/to/src/app.db> <path/to/dest/db>",
		Short: "Extracts the keys & values stored in the leaf nodes of an IAVL tree to a new DB",
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

			if version > 0 {
				fmt.Printf("Extracting keys & values from IAVL tree version %v in %s\n", version, srcDBPath)
			} else {
				fmt.Printf("Extracting keys & values from latest IAVL tree version in %s\n", srcDBPath)
			}
			start := time.Now()
			err = appstore.ExtractIAVLTreeValuesFromDB(srcDBPath, destDBPath, version, logLevel, batchSize)
			if err != nil {
				fmt.Printf("Failed to extract keys & values, time taken: %v mins\n", time.Now().Sub(start).Minutes())
				return err
			}
			fmt.Printf("Extracted keys & values, time taken: %v mins\n", time.Now().Sub(start).Minutes())
			return nil
		},
	}
	cmdFlags := cmd.Flags()
	cmdFlags.Int64Var(&version, "version", 0, "The IAVL tree version to extract keys & values from. Defaults to the latest tree.")
	cmdFlags.Int64Var(&logLevel, "log", 0, "How often progress output should be printed. 1 - every 10%, 2 - every 1%, 3 - every 0.1%.")
	cmdFlags.Int64Var(&batchSize, "batch-size", 10000, "Number of keys to write in each batch.")
	return cmd
}

func newAppStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "app-store",
		Short: "Tools that operate on the app store (app.db)",
	}
	cmd.AddCommand(
		newExtractValuesFromIAVLStoreCommand(),
		newCloneAppStoreCommand(),
		newTotalDataCommand(),
		newExtractEvmCommand(),
		newExtractEvmAuxCommand(),
	)
	return cmd
}
