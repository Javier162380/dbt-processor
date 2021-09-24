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

	var manifestPath string

	manifestPath, found := os.LookupEnv("DBT_PROFILES_DIR")

	if !found {
		manifestPath = "/target/manifest.json"
	}

	rootCmd := &cobra.Command{
		Use:   "dbt-processor",
		Short: "A useful CLI to get a control of your DBT project",
		Long: `The CLI to control your DBT project.
When DBT projects become big it is hard to control the dependencies between the different models.`,
	}

	f := rootCmd.Flags()
	f.StringVarP(&manifestPath, "manifest-path", "m", manifestPath, "Path were the dbt manifest it is located")
	rootCmd.AddCommand(buildTableCommand(manifestPath))

	return rootCmd, nil
}
