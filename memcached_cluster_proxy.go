package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/server"
)

func init() {
	// Set GOGC default explicitly
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	// Setting up signal handlers
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()

	// http debug and metrics endpoint
	go http.ListenAndServe("localhost:11299", nil)

	// metrics output prefix
	metrics.SetPrefix("rend_")
}

// Flags
var (
	clusterName string
	hostnames   string

	//	l1batched bool
	//	batchOpts batched.Opts

	//	l2enabled bool
	//	l2sock    string

	//	locked      bool
	//	concurrency int
	//	multiReader bool

	listenPort int

//	batchPort       int
//	useDomainSocket bool
//	sockPath        string
)

func init() {
	flag.IntVar(&listenPort, "p", 8080, "External port to listen on")
	flag.StringVar(&clusterName, "cluster-name", "mems99", "The consul service name of the cluster")
	flag.StringVar(&hostnames, "hostnames", "", "Force the cluster to be represented by those machines")

	flag.Parse()
}

// And away we go
func main() {
	l := server.TCPListener(listenPort)
	protocols := []protocol.Components{binprot.Components}

	var o orcas.OrcaConst
	var h2 handlers.HandlerConst
	var h1 handlers.HandlerConst

	memcachedInstances := strings.Split(hostnames, ",")
	if len(memcachedInstances) <= 0 || len(memcachedInstances[0]) <= 0 {
		panic("Cannot create a cluster of 0 nodes")
	}
	h1 = memcached.Cluster(memcachedInstances, clusterName)
	h2 = memcached.Cluster(memcachedInstances, clusterName)
	o = orcas.Backfill

	go server.ListenAndServe(l, protocols, server.Default, o, h1, h2)

	// Block forever
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
