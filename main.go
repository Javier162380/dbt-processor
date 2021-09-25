package main

import (
	"dbt-processor/cmd"
	"fmt"
	"time"
)

func main() {
	now := time.Now()
	defer func() {
		fmt.Println(time.Since(now).Seconds())
	}()
	cmd.Execute()
}
