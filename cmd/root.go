package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cmd, err := buildRootCommand()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func buildRootCommand() (*cobra.Command, error) {

	rootCmd := &cobra.Command{
		Use:   "dbt-processor",
		Short: "A useful CLI to get a control of your DBT project",
		Long: `The CLI to control your DBT project.
When DBT projects become big it is hard to control the dependencies between the different models.`,
	}

	rootCmd.AddCommand(buildTableCommand())

	return rootCmd, nil
}
