package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
)

type rawDBTManifest struct {
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

type processDBTManifest struct {
	ModelSources                              []string
	ModelUpstreamDependencies                 []string
	ModelDownstreamDependencies               []string
	ModelDownstreamDependenciesSecondHirarchy []string
	ModelTests                                []string
}

func containsElement(s []string, str string) bool {
	for _, v := range s {
		if strings.Contains(str, v) {
			return true
		}
	}

	return false
}

func fetchSources(Dependencies []string, maxEnitityWidth int) []string {

	sourceCollection := []string{}
	for _, v := range Dependencies {

		if strings.Contains(v, "source.") {
			if !containsElement(sourceCollection, v) {
				cleanSource := strings.Join(strings.Split(v, ".")[2:], ".")
				maxSourceLength := math.Min(float64(len(cleanSource)), float64(maxEnitityWidth))
				sourceCollection = append(sourceCollection, cleanSource[:int(maxSourceLength)])
			}
		}

	}

	return sourceCollection
}

func fetchModels(Dependencies []string, maxEnitityWidth float64) []string {

	modelsCollection := []string{}
	for _, v := range Dependencies {

		if strings.Contains(v, "model.") {
			if !containsElement(modelsCollection, v) {
				splitModel := strings.Split(v, ".")
				cleanModel := splitModel[len(splitModel)-1]
				maxModelLength := math.Min(float64(len(cleanModel)), maxEnitityWidth)
				modelsCollection = append(modelsCollection, cleanModel[:int(maxModelLength)])
			}

		}

	}

	return modelsCollection

}

func fetchTests(Dependencies []string, maxEnitityWidth float64) []string {

	testsCollection := []string{}

	for _, v := range Dependencies {
		if strings.Contains(v, "test.") {
			splitTest := strings.Split(v, ".")
			cleanTest := splitTest[len(splitTest)-2]
			maxTestLength := math.Min(float64(len(cleanTest)), maxEnitityWidth)
			testsCollection = append(testsCollection, cleanTest[:int(maxTestLength)])
		}
	}

	return testsCollection

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

func generateDBTProcessedManifest(rawManifest rawDBTManifest, modelName string, packageName string) processDBTManifest {

	modelFullName := generateDBTModelName(packageName, modelName)
	UpstreamDependencies := rawManifest.ParentMap[modelFullName]
	DownstreamDependencies := rawManifest.ChildMap[modelFullName]
	modelSources := fetchSources(UpstreamDependencies, 80.0)
	modelUpstreamDependencies := fetchModels(UpstreamDependencies, 50.0)
	modelDownstreamDependencies := fetchModels(DownstreamDependencies, 50.0)
	modelDownstreamDependenciesSecondHirarchy := fetchdonwstreamNestedDepsSecondHiesrarchy(DownstreamDependencies, rawManifest.ChildMap)
	modelTests := fetchTests(DownstreamDependencies, 50.0)

	processedManifest := processDBTManifest{}
	processedManifest.ModelSources = modelSources
	processedManifest.ModelUpstreamDependencies = modelUpstreamDependencies
	processedManifest.ModelDownstreamDependencies = modelDownstreamDependencies
	processedManifest.ModelDownstreamDependenciesSecondHirarchy = modelDownstreamDependenciesSecondHirarchy
	processedManifest.ModelTests = modelTests

	return processedManifest

}

func generateDBTManifestTableData(processedManifest processDBTManifest, modelName string) [][]string {

	bulkData := [][]string{}

	switch {
	case len(processedManifest.ModelSources) > 0:
		for index, v := range processedManifest.ModelSources {

			if index == 0 {
				bulkData = append(bulkData, []string{modelName, v, "", "", ""})
			} else {
				bulkData = append(bulkData, []string{"", v, "", "", ""})
			}

		}

		if len(processedManifest.ModelUpstreamDependencies) > 0 {
			for index, v := range processedManifest.ModelUpstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {
					bulkData[index][2] = v

				} else {
					bulkData = append(bulkData, []string{"", "", v, "", ""})
				}

			}
		}

		if len(processedManifest.ModelDownstreamDependencies) > 0 {
			for index, v := range processedManifest.ModelDownstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {

					bulkData[index][3] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", v, ""})
				}

			}
		}

		if len(processedManifest.ModelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range processedManifest.ModelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	case len(processedManifest.ModelUpstreamDependencies) > 0:
		for index, v := range processedManifest.ModelUpstreamDependencies {

			if index == 0 {
				bulkData = append(bulkData, []string{modelName, "", v, "", ""})
			} else {
				bulkData = append(bulkData, []string{"", "", v, "", ""})
			}
		}

		if len(processedManifest.ModelDownstreamDependencies) > 0 {
			for index, v := range processedManifest.ModelDownstreamDependencies {

				sliceLen := len(bulkData)
				if index < sliceLen {
					bulkData[index][3] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", v, ""})
				}

			}
		}

		if len(processedManifest.ModelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range processedManifest.ModelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	case len(processedManifest.ModelDownstreamDependencies) > 0:
		for index, v := range processedManifest.ModelDownstreamDependencies {
			if index == 0 {
				bulkData = append(bulkData, []string{modelName, "", "", v, ""})
			} else {
				bulkData = append(bulkData, []string{"", "", "", v, "s"})
			}

		}

		if len(processedManifest.ModelDownstreamDependenciesSecondHirarchy) > 0 {
			for index, v := range processedManifest.ModelDownstreamDependenciesSecondHirarchy {
				sliceLen := len(bulkData)

				if index < sliceLen {
					bulkData[index][4] = v

				} else {
					bulkData = append(bulkData, []string{"", "", "", "", v})
				}
			}
		}

	}

	switch {
	case len(processedManifest.ModelTests) > 0:
		for index, v := range processedManifest.ModelTests {
			sliceLen := len(bulkData)

			if index < sliceLen {
				bulkData[index] = append(bulkData[index], v)
			} else {
				bulkData = append(bulkData, []string{"", "", "", "", "", v})
			}

		}

	}

	return bulkData
}

func BuildDBTManifestTable(manifestPath string, modelName string, packageName string) *tablewriter.Table {

	rawManifest := rawDBTManifest{}

	file, _ := ioutil.ReadFile(manifestPath)
	_ = json.Unmarshal([]byte(file), &rawManifest)

	processedManifest := generateDBTProcessedManifest(rawManifest, modelName, packageName)
	bulkData := generateDBTManifestTableData(processedManifest, modelName)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Model Name", "Model Sources", "Upstream Dependencies", "Downstream Dependencies", "Downstream Deps of Deps", "Model Tests"})
	table.SetFooter([]string{"TOTAL", fmt.Sprintf("%d", len(processedManifest.ModelSources)), fmt.Sprintf("%d", len(processedManifest.ModelUpstreamDependencies)), fmt.Sprintf("%d", len(processedManifest.ModelDownstreamDependencies)), fmt.Sprintf("%d", len(processedManifest.ModelDownstreamDependenciesSecondHirarchy)), fmt.Sprintf("%d", len(processedManifest.ModelTests))})

	table.AppendBulk(bulkData)

	return table

}
