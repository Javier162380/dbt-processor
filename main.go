package main

import (
	"dbt-processor/cmd"
	"fmt"
	"time"
)

func main() {
	now := time.Now()
	defer func() {
		fmt.Printf("Execution Time: %v seconds", time.Since(now).Seconds())
	}()
	cmd.Execute()
}
