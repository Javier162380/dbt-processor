package cmd

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidTableCommand(t *testing.T) {
	testcases := []struct {
		Input    []string
		Output   string
		TestCase string
	}{{
		Input: []string{"table", "-n", "stg_trf_revenue_per_sales_day", "-p", "wetransfer"}}}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Test Create %s", tc.TestCase), func(t *testing.T) {
			b := new(bytes.Buffer)
			rootCmd, _ := buildRootCommand()
			rootCmd.SetArgs(tc.Input)
			rootCmd.SetOut(b)
			rootCmd.SetErr(b)
			err := rootCmd.Execute()
			assert.NoError(t, err)
		})

	}
}
