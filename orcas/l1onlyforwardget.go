// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orcas

import (
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/timer"
)

type L1OnlyForwardGetOrca struct {
	l1  handlers.Handler
	res protocol.Responder
}

func L1OnlyForwardGet(l1, l2 handlers.Handler, res protocol.Responder) Orca {
	return &L1OnlyForwardGetOrca{
		l1:  l1,
		res: res,
	}
}

func (l *L1OnlyForwardGetOrca) Set(req common.SetRequest) error {
	// We don't care of SET, we just swallow them and return a success
	l.res.Set(req.Opaque, req.Quiet)
	return nil
}


func (l *L1OnlyForwardGetOrca) Get(req common.GetRequest) error {
	metrics.IncCounterBy(MetricCmdGetKeys, uint64(len(req.Keys)))
	//debugString := "get"
	//for _, k := range req.Keys {
	//	debugString += " "
	//	debugString += string(k)
	//}
	//println(debugString)

	metrics.IncCounter(MetricCmdGetL1)
	metrics.IncCounterBy(MetricCmdGetKeysL1, uint64(len(req.Keys)))
	start := timer.Now()

	resChan, errChan := l.l1.Get(req)

	var err error

	// Read all the responses back from l.l1.
	// The contract is that the resChan will have GetResponse's for get hits and misses,
	// and the errChan will have any other errors, such as an out of memory error from
	// memcached. If any receive happens from errChan, there will be no more responses
	// from resChan.
	for {
		select {
		case res, ok := <-resChan:
			if !ok {
				resChan = nil
			} else {
				if res.Miss {
					metrics.IncCounter(MetricCmdGetMissesL1)
					metrics.IncCounter(MetricCmdGetMisses)
				} else {
					metrics.IncCounter(MetricCmdGetHits)
					metrics.IncCounter(MetricCmdGetHitsL1)
				}
				l.res.Get(res)
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(MetricCmdGetErrors)
				metrics.IncCounter(MetricCmdGetErrorsL1)
				err = getErr
			}
		}

		if resChan == nil && errChan == nil {
			break
		}
	}

	metrics.ObserveHist(HistGetL1, timer.Since(start))

	if err == nil {
		l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	return err
}

func (l *L1OnlyForwardGetOrca) Noop(req common.NoopRequest) error {
	return l.res.Noop(req.Opaque)
}

func (l *L1OnlyForwardGetOrca) Quit(req common.QuitRequest) error {
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1OnlyForwardGetOrca) Version(req common.VersionRequest) error {
	return l.res.Version(req.Opaque)
}

func (l *L1OnlyForwardGetOrca) Unknown(req common.Request) error {
	return common.ErrUnknownCmd
}

func (l *L1OnlyForwardGetOrca) Error(req common.Request, reqType common.RequestType, err error) {
	var opaque uint32
	var quiet bool

	if req != nil {
		opaque = req.GetOpaque()
		quiet = req.IsQuiet()
	}

	l.res.Error(opaque, reqType, err, quiet)
}

func (l *L1OnlyForwardGetOrca) Add(req common.SetRequest) error                                 { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Replace(req common.SetRequest) error                             { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Append(req common.SetRequest) error                              { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Prepend(req common.SetRequest) error                             { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Delete(req common.DeleteRequest) error                           { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Touch(req common.TouchRequest) error                             { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) GetE(req common.GetRequest) error                                { return common.ErrNoError }
func (l *L1OnlyForwardGetOrca) Gat(req common.GATRequest) error                                 { return common.ErrNoError }

