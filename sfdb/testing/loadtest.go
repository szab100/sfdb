package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"flag"

	log "github.com/golang/glog"

	api "github.com/googlegsa/sfdb/api_go_proto"
	"google.golang.org/grpc"
)

var (
	sfdbTarget   = flag.String("sfdb_target", "127.0.0.1:27910", "Test target")
	numRows      = flag.Int("num_rows", 1000, "Number of rows to create in the load test table")
	insertQPS    = flag.Int("insert_qps", 50, "Number of row insertions per second")
	heartbeatQPS = flag.Int("heartbeat_qps", -1, "Number of heartbeats per second")
)

func execSql(stub api.SfdbServiceClient, sql string, done chan error) {
	req := &api.ExecSqlRequest{Sql: &sql}
	_, err := stub.ExecSql(context.Background(), req)
	if err != nil {
		log.Error("ExecSql(", sql, ") failed with ", err)
	}
	if done != nil {
		done <- err
	}
}

func execSqlOrDie(stub api.SfdbServiceClient, sql string, choke chan interface{}) {
	done := make(chan error, 1)
	execSql(stub, sql, done)
	if <-done != nil {
		os.Exit(1)
	}
	if choke != nil {
		<-choke
	}
}

func vmID(i int) string {
	return fmt.Sprintf("edge/sfo-shard-%d/%08x", i%4, i^0xdeadbeef)
}

func streamInsertions(sqls chan string) {
	log.Info("Inserting rows...")
	for i := 0; i < *numRows; i++ {
		sql := fmt.Sprintf("INSERT INTO LoadTest (vm_id, location) VALUES ('%s', 'sfo');", vmID(i))
		sqls <- sql
		time.Sleep(time.Second / time.Duration(*insertQPS))
	}
}

func streamHeartbeats(sqls chan string) {
	log.Info("Sending heartbeats...")
	for {
		for i := 0; i < *numRows; i++ {
			nowNanos := time.Now().UnixNano()
			sql := fmt.Sprintf("UPDATE LoadTest SET heartbeat_nanos = %d WHERE vm_id = '%s';", nowNanos, vmID(i))
			sqls <- sql
			time.Sleep(time.Second / time.Duration(*heartbeatQPS))
		}
	}
}

func execSqls(stub api.SfdbServiceClient, sqls chan string) {
	lastCheckpoint := time.Now()
	rpcsSinceLastCheckpoint := 0

	choke := make(chan interface{}, 20) // max number of concurrent RPCs
	for {
		sql, more := <-sqls
		if !more {
			break
		}
		choke <- nil
		go execSqlOrDie(stub, sql, choke)

		// Print some stats
		rpcsSinceLastCheckpoint += 1
		now := time.Now()
		if now.Sub(lastCheckpoint) >= time.Second {
			seconds := int(now.Sub(lastCheckpoint)/time.Millisecond) / 1000.0
			log.Infof("%v QPS; %v in flight", rpcsSinceLastCheckpoint/seconds, len(choke))
			lastCheckpoint = now
			rpcsSinceLastCheckpoint = 0
		}
	}
}

func main() {
	flag.Parse()

	if *heartbeatQPS < 0 {
		// The default heartbeat rate is once per 50 seconds for each VM.
		*heartbeatQPS = *numRows / 50
	}

	// Create RPC stub.
	log.Info("Creating RPC client for ", *sfdbTarget)
	conn, err := grpc.Dial(*sfdbTarget, grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	defer conn.Close()
	stub := api.NewSfdbServiceClient(conn)

	log.Info("Initializing the table")
	execSql(stub, "DROP TABLE LoadTest;", nil) // ignore errors
	execSqlOrDie(stub, "CREATE TABLE LoadTest (vm_id string, location string, heartbeat_nanos int64);", nil)
	execSqlOrDie(stub, "CREATE INDEX ById ON LoadTest (vm_id);", nil)

	sqls := make(chan string, 100)
	go execSqls(stub, sqls)

	streamInsertions(sqls)
	streamHeartbeats(sqls)
}
