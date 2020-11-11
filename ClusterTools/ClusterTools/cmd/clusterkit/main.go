package main

import (
	"fmt"
	"os"

	"github.com/dappchain/clusterkit/version"
	"github.com/spf13/cobra"
)

func newVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Display the clusterkit version",
		RunE: func(cmd *cobra.Command, args []string) error {
			println(version.FullVersion())
			return nil
		},
	}
	return cmd
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "clusterkit",
		Short: "DAppChain maintenance tools",
	}

	rootCmd.AddCommand(
		newVersionCommand(),
		newAppStoreCommand(),
		newBlockStoreCommand(),
	)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
