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
	"reflect"

	"database/sql"

	_ "github.com/googlegsa/sfdb/driver/go"

	log "github.com/golang/glog"
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

func value2str(ct sql.ColumnType, val interface{}) string {
	if ct.ScanType().ConvertibleTo(reflect.TypeOf(int64(0))) || ct.ScanType().ConvertibleTo(reflect.TypeOf(uint64(0))) {
		if typed_val, ok := val.(int64); ok {
			return fmt.Sprintf("%d", typed_val)
		}
	} else if ct.ScanType().ConvertibleTo(reflect.TypeOf(bool(false))) {
		if typed_val, ok := val.(bool); ok {
			if typed_val {
				return "true"
			} else {
				return "false"
			}
		}
	} else if ct.ScanType().ConvertibleTo(reflect.TypeOf(string(""))) {
		if typed_val, ok := val.(string); ok {
			return typed_val
		}
	}

	return "<invalid value>"
}

func IMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func IMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func drawTable(rows *sql.Rows) {
	columns, err := rows.Columns()
	if err != nil {
		fmt.Print("Error: no columns!\n")
		return
	}

	column_types, err := rows.ColumnTypes()
	if err != nil {
		fmt.Print("Error: no columns!\n")
		return
	}

	const column_max_width = 20
	var num_columns = len(columns)

	if num_columns == 0 {
		fmt.Print("<Empty results>\n")
		return
	}

	// Calculate actual column widths based on column names
	col_widths := make([]int, num_columns)
	for i, column := range columns {
		col_widths[i] = IMin(column_max_width, len(column))
	}

	// Extract text from rows and recalculate column widths
	var row_str_vals [][]string
	for rows.Next() {
		row_contents := make([]interface{}, num_columns)
		row_contents_ptr := make([]interface{}, num_columns)

		for i, _ := range row_contents_ptr {
			row_contents_ptr[i] = &row_contents[i]
		}

		if err := rows.Scan(row_contents_ptr...); err != nil {
			// TODO: return with error or type some error in table
			continue
		}

		// Crete text representation of row columns.
		// Make columns to be of length 20, add special symbols
		// to draw table
		str_vals := make([]string, num_columns)
		for i, _ := range str_vals {
			str_vals[i] = value2str(*column_types[i], row_contents[i])
			col_widths[i] = IMin(column_max_width, IMax(col_widths[i], len(str_vals[i])))
		}
		row_str_vals = append(row_str_vals, str_vals)
	}

	var table_horizontal_edge string
	var heading string

	for i, str_val := range columns {
		max_text_len := col_widths[i]
		val_len := len(str_val)

		if val_len > max_text_len {
			str_val = " " + str_val[:(max_text_len - 3)] + "... "
		} else {
			str_val = fmt.Sprintf(" %s%s ", str_val, strings.Repeat("\x20", max_text_len - val_len));
		}

		table_horizontal_edge += "+" + strings.Repeat("-", max_text_len + 2)
		heading += "|" + str_val

		if (i == (num_columns - 1)) {
			table_horizontal_edge += "+"
			heading += "|"
		}
	}

	var body string

	for _, str_vals := range row_str_vals {
		// Crete text representation of row columns.
		// Make columns to be of length 20, add special symbols
		// to draw table
		var row string
		for i, str_val := range str_vals {
			max_text_len := col_widths[i]
			val_len := len(str_val)

			if val_len > max_text_len {
				str_val = " " + str_val[:(max_text_len - 3)] + "... "
			} else {
				str_val = fmt.Sprintf(" %s%s ", str_val, strings.Repeat("\x20", max_text_len - val_len));
			}

			row += "|" + str_val
			if (i == (num_columns - 1)) {
				row += "|"
			}
		}
		if len(body) > 0 {
			body += "\n"
		}
		body += row
	}

	fmt.Printf("\n%s\n", table_horizontal_edge)
	fmt.Print(heading)
	fmt.Printf("\n%s\n", table_horizontal_edge)
	fmt.Print(body)
	fmt.Printf("\n%s\n", table_horizontal_edge)
}

/* SFDB client */

type sfdbClient struct {
	db *sql.DB
}

// Creates a protobuf and sends RPC request to SFDB target.
func (c *sfdbClient) execSQL(sql string) error {
	// include debug strings in RPC response
	rows, err := c.db.Query(sql)
	if err != nil {
		return err
	}

	defer rows.Close()

	drawTable(rows)

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
			fmt.Printf("Failed to read stdin: %s\n", err.Error())
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
				fmt.Printf("DB returned error: %s\n", err.Error())
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
					fmt.Printf("DB returned error: %s", e.Error())
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
				fmt.Printf("DB returned error: %s", err.Error())
			}
		}
	}
}

func main() {
	flag.Parse()

	// 1. Init the connection to SFDB target
	// log.Info("Establishing RPC connection for ", *sfdbTarget)
	fmt.Printf("Establishing RPC connection for %s\n", *sfdbTarget);
	var err error

	db, err := sql.Open("sfdb", *sfdbTarget)

	if err != nil {
		fmt.Printf("Failed to connect: %s\n", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	// 1.1 Handle SIGINT
	sigint_ch := make(chan os.Signal)
	signal.Notify(sigint_ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigint_ch
		fmt.Println("Closing RPC gracefully..")
		db.Close()
		os.Exit(0)
	}()

	client := &sfdbClient{db}

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
