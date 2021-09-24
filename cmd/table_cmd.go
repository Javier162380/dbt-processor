package cmd

import (
	internal "dbt-processor/internal/dbt"

	"github.com/spf13/cobra"
)

func buildTableCommand(manifestPath string) *cobra.Command {

	var modelName string
	var projectName string

	createTableCmd := &cobra.Command{
		Use:   "table",
		Short: "Crete a new table with the dependencies information of the different models",
		Long:  `Create a new task in the togolist`,
		Run: func(cmd *cobra.Command, args []string) {
			internal.BuildDBTManifestTable(manifestPath, modelName, projectName)
		},
	}

	f := createTableCmd.Flags()
	f.StringVarP(&modelName, "model-name", "n", "", "The name of the model we are trying to analyze")
	f.StringVarP(&projectName, "project-name", "p", "", "The name of the project for the model")

	return createTableCmd
}
