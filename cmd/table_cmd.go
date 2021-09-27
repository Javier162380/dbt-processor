package cmd

import (
	internal "dbt-processor/internal/dbt"

	"github.com/spf13/cobra"
)

func buildTableCommand() *cobra.Command {

	var modelName string
	var projectName string
	var manifestPath string

	defaultManifest := "/target/manifest.json"

	createTableCmd := &cobra.Command{
		Use:   "table",
		Short: "Crete a new table with the dependencies information of the different models",
		Long:  `Create a new task in the togolist`,
		Run: func(cmd *cobra.Command, args []string) {
			table := internal.BuildDBTManifestTable(manifestPath, modelName, projectName)
			table.Render()
		},
	}

	f := createTableCmd.Flags()
	f.StringVarP(&modelName, "model-name", "n", "", "The name of the model we are trying to analyze")
	f.StringVarP(&projectName, "project-name", "p", "", "The name of the project for the model")
	f.StringVarP(&manifestPath, "manifest-path", "m", defaultManifest, "Abs path where the manifest it is located")

	return createTableCmd
}
