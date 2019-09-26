/*
 * Copyright (c) 2019 Google LLC.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/golang/glog"
	api "github.com/googlegsa/sfdb/api_go_proto"
	"google.golang.org/grpc"
)

const (
	version = "0.0.1a"
)

var (
	// sfdb RPC server address (host, ip:port)
	sfdbTarget = flag.String("host", "127.0.0.1:27910", "SFDB target (host | ip:port)")
	// absolute filepath to SQL file to read from instead os.Stdin
	sqlFile = flag.String("filepath", "", "Abs. filepath of SQL to execute commands from.")
	// if commands (reading from input file) can be executed async.
	// Useful for SELECT, INSERT
	async = flag.Bool("async", false, "Boolean - when 'true', all SQL commands are executed asynchronously. Default: false.")
	// regex to split column delimeters of debug strings in Proto response.
	colSplitRgx = regexp.MustCompile(`_\d:\s`)
)

/* Utils */

func sanitizeStr(s string) string {
	t := strings.TrimSpace(s)
	return strings.TrimSuffix(t, "\n")
}

func getMaxStrLen(array []string) int {
	var max = len(array[0])
	for _, str := range array {
		if max < len(str) {
			max = len(str)
		}
	}
	return max
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func isDigit(v string) bool {
	_, err := strconv.ParseInt(v, 10, 64)
	return err == nil
}

// queryResult struct contains parsed result from SFDB query
type queryResult struct {
	// Structured SQL query results, instead of string,
	// e.g.: -> {row_index: ["val1", "val2", "val3"..]
	Data *map[int][]string
	// Max length of row value per column,
	// will be used for padding when we draw table.
	MaxLen []int
}

func (result *queryResult) drawTable() {
	// iterate over data, padding with 0x20 (space)
	rowsToPrint := []string{}
	for _, rowVals := range *result.Data {
		row := ""
		for i, rowVal := range rowVals {
			padding := 0
			if !isDigit(rowVal) {
				// append 2 bytes of 0x20 due to quotes
				padding += 2
			}

			// trim long row values longer than 19 chars
			if len(rowVal) > 19 {
				rowVal = rowVal[:19] + "..."
			}

			if diff := result.MaxLen[i] - len(rowVal) + padding; diff > 0 {
				rowVal += strings.Repeat("\x20", diff)
			}
			row += "\x20" + rowVal + "\x20|"
		}
		rowsToPrint = append(rowsToPrint, row)
	}

	// determine the absolute max length of the row
	absMaxlen := getMaxStrLen(rowsToPrint)

	// print header
	fmt.Printf("\n+%s+\n", strings.Repeat("-", absMaxlen-1))

	// print rows
	for _, row := range rowsToPrint {
		fmt.Printf("|%s\n", row)
	}

	// print footer
	fmt.Printf("+%s+\n", strings.Repeat("-", absMaxlen-1))
}

// Given a slice of strings where each string is an query result,
// delimited with "_{1,2..}:", we can split into the structured data
// like queryResult struct and also define here the maximum length
// per each column values in order to print a padded table.
func splitRowPerColumn(rows []string) *queryResult {
	// 1. Find column delimiters
	// TODO: Can not parse strings with /_/d:/ pattern.
	colDelims := colSplitRgx.FindAllString(rows[0], -1)

	// 2. Compile regexps to find Column and Value from row string
	colDelimsRgxp := []*regexp.Regexp{}
	for _, colDelim := range colDelims {
		colSplitRgxp := regexp.MustCompile(fmt.Sprintf(`(%s\W?\w+\W?)`, colDelim))
		colDelimsRgxp = append(colDelimsRgxp, colSplitRgxp)
	}

	// 3. Fetch Column and Value via regexp per row into map
	dict := make(map[int]int)
	table := make(map[int][]string)

	for i, row := range rows {
		var vals []string

		for j, colSplitRgxp := range colDelimsRgxp {
			// Get the value per column from a row string:
			// example, "_1: 1234 _2: 1337" -> "_1: 1234"
			text := colSplitRgxp.FindString(row)
			// Now get only the value without "_d:\s" prefix:
			// example, "_1: 1234" -> "1234"
			text = strings.Replace(text, string(colDelims[j]), "", -1)
			text_ := sanitizeStr(text)

			vals = append(vals, text_)

			if _, ok := dict[j]; ok {
				dict[j] = Max(dict[j], len(text))
			} else {
				dict[j] = len(text)
			}
		}

		table[i] = vals
	}

	// 4. Dict to slice of max length per column
	maxLenPerColumn := []int{}
	for _, lv := range dict {
		maxLenPerColumn = append(maxLenPerColumn, lv)
	}

	result := queryResult{
		Data:   &table,
		MaxLen: maxLenPerColumn,
	}

	return &result
}

/* SFDB client */

type sfdbClient struct {
	stub api.SfdbServiceClient
}

// Creates a protobuf and sends RPC request to SFDB target.
func (c *sfdbClient) execSQL(sql string) error {
	// include debug strings in RPC response
	includeDebugStrings := true

	protoreq := api.ExecSqlRequest{
		Sql:                 &sql,
		IncludeDebugStrings: &includeDebugStrings,
	}

	protoresp, err := c.stub.ExecSql(context.Background(), &protoreq)
	if err != nil {
		return err
	}

	// TODO: refactor client to use our Go driver when it becomes 100% ready
	if len(protoresp.Rows) > 0 {
		pResult := splitRowPerColumn(protoresp.DebugStrings)
		pResult.drawTable()
	}
	return nil
}

// In infinite loop, get STDIN and send RPC requests
func (c *sfdbClient) execFromStdin() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("sfdb> ")
		// TODO: ReadString buffer is not limited
		cmd, err := reader.ReadString('\n')
		if len(cmd) == 0 && err != nil {
			if err == io.EOF {
				break
			}
			log.Error(err)
		}

		cmd_ := sanitizeStr(cmd)
		if cmd_ == "" {
			continue
		}

		// single commands
		switch cmd_ {
		case "exit", "quit", "q":
			// TODO: Close the connection
			os.Exit(0)
		default:
			err := c.execSQL(cmd_)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (c *sfdbClient) execFromFile() {
	fd, err := os.Open(*sqlFile)
	if err != nil {
		log.Error(err)
	}
	defer fd.Close()

	var lines []string
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err = scanner.Err(); err != nil {
		log.Error(err)
	}

	fmt.Println("[+] Executing from input file.")

	if *async {
		chanErr := make(chan error, len(lines))
		var wg sync.WaitGroup
		wg.Add(len(lines))

		for _, line := range lines {
			go func(line string) {
				defer wg.Done()

				line_ := sanitizeStr(line)
				if line_ == "" {
					chanErr <- nil
				}

				chanErr <- c.execSQL(line_)
			}(line)
		}
		wg.Wait()

		for range lines {
			select {
			case <-time.After(10 * time.Second):
				fmt.Println("Too long")
			case e := <-chanErr:
				if e != nil {
					fmt.Println(e)
				}
			}
		}
	} else {
		for _, line := range lines {
			line_ := sanitizeStr(line)
			if line_ == "" {
				continue
			}
			err = c.execSQL(line_)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func main() {
	flag.Parse()

	// 1. Init the connection to SFDB target
	log.Info("Establishing RPC connection for ", *sfdbTarget)
	var err error
	conn, err := grpc.Dial(*sfdbTarget, grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	defer conn.Close()

	// 1.1 Handle SIGINT
	sigint_ch := make(chan os.Signal)
	signal.Notify(sigint_ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigint_ch
		fmt.Println("Closing RPC gracefully..")
		conn.Close()
		os.Exit(0)
	}()

	// 2. Create the RPC stub
	log.Info("Creating RPC client..")
	stub := api.NewSfdbServiceClient(conn)
	client := &sfdbClient{stub}

	welcome()

	// 3. Exec SQL either way
	if *sqlFile != "" {
		client.execFromFile()
	} else {
		client.execFromStdin()
	}
}

func welcome() {
	var msg = `
Welcome to the SFDB client.  Commands end with ;.
Client version: %s.

Use "$ man -l sfdb_cli.1" for manual page.

Type 'exit', 'quit' or 'q' to quit.
`
	fmt.Printf(msg, version)
}
