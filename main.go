package main

import (
	"dbt-processor/cmd"
	"fmt"
	"strings"
	"time"
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

func main() {

	cmd.Execute()
}
