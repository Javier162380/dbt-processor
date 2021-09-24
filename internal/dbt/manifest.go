package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

type DBTManifest struct {
	Metadata struct {
		DbtSchemaVersion string    `json:"dbt_schema_version"`
		DbtVersion       string    `json:"dbt_version"`
		GeneratedAt      time.Time `json:"generated_at"`
		InvocationID     string    `json:"invocation_id"`
		Env              struct {
		} `json:"env"`
		ProjectID               string `json:"project_id"`
		UserID                  string `json:"user_id"`
		SendAnonymousUsageStats bool   `json:"send_anonymous_usage_stats"`
		AdapterType             string `json:"adapter_type"`
	} `json:"metadata"`
	Nodes     map[string]interface{} `json:"nodes"`
	Sources   map[string]interface{} `json:"sources"`
	Macros    map[string]interface{} `json:"macros"`
	Docs      map[string]interface{} `json:"docs"`
	Exposures struct {
	} `json:"exposures"`
	Selectors struct {
	} `json:"selectors"`
	Disabled  []map[string]interface{} `json:"disabled"`
	ParentMap map[string][]string      `json:"parent_map"`
	ChildMap  map[string][]string      `json:"child_map"`
}

func containsElement(s []string, str string) bool {
	for _, v := range s {
		if strings.Contains(str, v) {
			return true
		}
	}

	return false
}

func fetchSources(Dependencies []string) []string {

	sourceCollection := []string{}
	for _, v := range Dependencies {

		if strings.Contains(v, "source.") {
			if !containsElement(sourceCollection, v) {
				cleanSource := strings.Join(strings.Split(v, ".")[2:], ".")
				sourceCollection = append(sourceCollection, cleanSource)
			}
		}

	}

	return sourceCollection
}

func fetchModels(Dependencies []string) []string {

	sourceCollection := []string{}
	for _, v := range Dependencies {

		if strings.Contains(v, "model.") {
			if !containsElement(sourceCollection, v) && !strings.Contains(v, "test.") {
				splitModel := strings.Split(v, ".")
				cleanModel := splitModel[len(splitModel)-1]
				sourceCollection = append(sourceCollection, cleanModel)
			}

		}

	}

	return sourceCollection

}

func fetchdonwstreamNestedDepsSecondHiesrarchy(dependencies []string, chilMap map[string][]string) []string {

	nestedDepsSecondHierarchy := []string{}

	if len(dependencies) > 0 {
		for _, dependency := range dependencies {

			if nestedDeps, found := chilMap[dependency]; found {

				for _, v := range nestedDeps {
					if !containsElement(nestedDepsSecondHierarchy, v) && !strings.Contains(v, "test.") {
						splitModel := strings.Split(v, ".")
						cleanModel := splitModel[len(splitModel)-1]
						nestedDepsSecondHierarchy = append(nestedDepsSecondHierarchy, cleanModel)
					}
				}

			}

		}

	}

	return nestedDepsSecondHierarchy

}

func generateDBTModelName(packageName string, modelName string) string {
	return fmt.Sprintf("model.%s.%s", packageName, modelName)
}

func BuildDBTManifestTable(manifestPath string, modelName string, packageName string) {

	dbtManifest := DBTManifest{}

	file, _ := ioutil.ReadFile(manifestPath)
	_ = json.Unmarshal([]byte(file), &dbtManifest)

	modelFullName := generateDBTModelName(packageName, modelName)
	UpstreamDependencies := dbtManifest.ParentMap[modelFullName]
	DownstreamDependencies := dbtManifest.ChildMap[modelFullName]
	modelSources := fetchSources(UpstreamDependencies)
	modelUpstreamDependencies := fetchModels(UpstreamDependencies)
	modelDownstreamDependencies := fetchModels(DownstreamDependencies)
	modelDownstreamDependenciesSecondHirarchy := fetchdonwstreamNestedDepsSecondHiesrarchy(DownstreamDependencies, dbtManifest.ChildMap)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Model Name", "Model Sources", "Upstream Dependencies", "Downstream Dependencies", "Downstream Deps of Deps"})
	table.SetFooter([]string{"TOTAL", fmt.Sprintf("%d", len(modelSources)), fmt.Sprintf("%d", len(modelUpstreamDependencies)), fmt.Sprintf("%d", len(modelDownstreamDependencies)), fmt.Sprintf("%d", len(modelDownstreamDependenciesSecondHirarchy))})
	bulkData := [][]string{}

	switch {
	case len(modelSources) > 0:
		for index, v := range modelSources {

			if index == 0 {
				bulkData = append(bulkData, []string{modelName, v, "", "", ""})
			} else {
				bulkData = append(bulkData, []string{"", v, "", "", ""})
			}

		}

		if len(modelUpstreamDependencies) > 0 {
			for index, v := range modelUpstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {
					bulkData[index][2] = v

				} else {
					bulkData = append(bulkData, []string{"", "", v, "", ""})
				}

			}
		}

		if len(modelDownstreamDependencies) > 0 {
			for index, v := range modelDownstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {

					bulkData[index][3] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", v, ""})
				}

			}
		}

		if len(modelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range modelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	case len(UpstreamDependencies) > 0:
		for index, v := range modelUpstreamDependencies {

			if index == 0 {
				bulkData = append(bulkData, []string{modelName, "", v, "", ""})
			} else {
				bulkData = append(bulkData, []string{"", "", v, "", ""})
			}
		}

		if len(modelDownstreamDependencies) > 0 {
			for index, v := range modelDownstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {
					bulkData[index][3] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", v, ""})
				}

			}
		}

		if len(modelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range modelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	case len(DownstreamDependencies) > 0:
		for index, v := range modelDownstreamDependencies {
			if index == 0 {
				bulkData = append(bulkData, []string{modelName, "", "", v, ""})
			} else {
				bulkData = append(bulkData, []string{"", "", "", v, "s"})
			}

		}

		if len(modelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range modelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	}

	table.AppendBulk(bulkData)
	table.Render()
}
