// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// StreamID identifies a stream; it may be local to a flow or it may cross
// machine boundaries. The identifier can only be used in the context of a
// specific flow.
type StreamID int

// FlowID identifies a flow. It is most importantly used when setting up streams
// between nodes.
type FlowID struct {
	uuid.UUID
}

// FlowCtx encompasses the contexts needed for various flow components.
type FlowCtx struct {
	Context context.Context
	id      FlowID
	evalCtx *parser.EvalContext
	rpcCtx  *rpc.Context
	txn     *client.Txn
}

type flowStatus int

// Flow status indicators.
const (
	FlowNotStarted flowStatus = iota
	FlowRunning
	FlowFinished
)

// Flow represents a flow which consists of processors and streams.
type Flow struct {
	FlowCtx

	flowRegistry *flowRegistry
	processors   []processor
	outboxes     []*outbox
	// simpleFlowConsumer is a special outbox which instead of sending rows to
	// another host, returns them directly (as a result to a SetupSimpleFlow RPC,
	// or to the local host).
	simpleFlowConsumer RowReceiver

	localStreams map[StreamID]RowReceiver

	// inboundStreams are streams that receive data from other hosts; this map
	// is to be passed to flowRegistry.RegisterFlow.
	inboundStreams map[StreamID]*inboundStreamInfo

	// waitGroup is used to wait for async components of the flow:
	//  - processors
	//  - inbound streams
	//  - outboxes
	waitGroup sync.WaitGroup

	doneFn func()

	status flowStatus
}

func newFlow(flowCtx FlowCtx, flowReg *flowRegistry, simpleFlowConsumer RowReceiver) *Flow {
	if opentracing.SpanFromContext(flowCtx.Context) == nil {
		panic("flow context has no span")
	}
	flowCtx.Context = log.WithLogTagStr(flowCtx.Context, "f", flowCtx.id.Short())
	f := &Flow{
		FlowCtx:            flowCtx,
		flowRegistry:       flowReg,
		simpleFlowConsumer: simpleFlowConsumer,
	}
	f.status = FlowNotStarted
	return f
}

// setupInboundStream adds a stream to the stream map (inboundStreams or
// localStreams).
func (f *Flow) setupInboundStream(spec StreamEndpointSpec, receiver RowReceiver) error {
	sid := spec.StreamID
	if spec.Mailbox != nil {
		if spec.Mailbox.SimpleResponse || spec.Mailbox.TargetAddr != "" {
			return errors.Errorf("inbound stream has SimpleResponse or TargetAddr set")
		}
		if _, found := f.inboundStreams[sid]; found {
			return errors.Errorf("inbound stream %d has multiple consumers", sid)
		}
		if f.inboundStreams == nil {
			f.inboundStreams = make(map[StreamID]*inboundStreamInfo)
		}
		if log.V(2) {
			log.Infof(f.FlowCtx.Context, "set up inbound stream %d", sid)
		}
		f.inboundStreams[sid] = &inboundStreamInfo{receiver: receiver, waitGroup: &f.waitGroup}
		return nil
	}
	if _, found := f.localStreams[sid]; found {
		return errors.Errorf("local stream %d has multiple consumers", sid)
	}
	if f.localStreams == nil {
		f.localStreams = make(map[StreamID]RowReceiver)
	}
	f.localStreams[sid] = receiver
	return nil
}

// setupOutStream sets up an output stream; if the stream is local, the
// RowChannel is looked up in the localStreams map; otherwise an outgoing
// mailbox is created.
func (f *Flow) setupOutStream(spec StreamEndpointSpec) (RowReceiver, error) {
	sid := spec.StreamID
	if spec.Mailbox != nil {
		if spec.Mailbox.SimpleResponse {
			return f.simpleFlowConsumer, nil
		}
		outbox := newOutbox(&f.FlowCtx, spec.Mailbox.TargetAddr, f.id, sid)
		f.outboxes = append(f.outboxes, outbox)
		return outbox, nil
	}
	rowChan, found := f.localStreams[sid]
	if !found {
		return nil, errors.Errorf("unconnected inbound stream %d", sid)
	}
	// Once we "connect" a stream, we set the value in the map to nil.
	if rowChan == nil {
		return nil, errors.Errorf("stream %d has multiple connections", sid)
	}
	f.localStreams[sid] = nil
	return rowChan, nil
}

func (f *Flow) setupRouter(spec *OutputRouterSpec) (RowReceiver, error) {
	streams := make([]RowReceiver, len(spec.Streams))
	for i := range spec.Streams {
		var err error
		streams[i], err = f.setupOutStream(spec.Streams[i])
		if err != nil {
			return nil, err
		}
	}
	return makeRouter(spec, streams)
}

func checkNumInOut(inputs []RowSource, outputs []RowReceiver, numIn, numOut int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	if len(outputs) != numOut {
		return errors.Errorf("expected %d output(s), got %d", numOut, len(outputs))
	}
	return nil
}

func (f *Flow) makeProcessor(ps *ProcessorSpec, inputs []RowSource) (processor, error) {
	if len(ps.Output) != 1 {
		return nil, errors.Errorf("only single-output processors supported")
	}
	outputs := make([]RowReceiver, len(ps.Output))
	for i := range ps.Output {
		var err error
		outputs[i], err = f.setupRouter(&ps.Output[i])
		if err != nil {
			return nil, err
		}
	}
	if ps.Core.Noop != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(&f.FlowCtx, inputs[0], outputs[0]), nil
	}
	if ps.Core.TableReader != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newTableReader(&f.FlowCtx, ps.Core.TableReader, outputs[0])
	}
	if ps.Core.JoinReader != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newJoinReader(&f.FlowCtx, ps.Core.JoinReader, inputs[0], outputs[0])
	}
	if ps.Core.Sorter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSorter(&f.FlowCtx, ps.Core.Sorter, inputs[0], outputs[0]), nil
	}
	if ps.Core.Evaluator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newEvaluator(&f.FlowCtx, ps.Core.Evaluator, inputs[0], outputs[0])
	}
	if ps.Core.Distinct != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newDistinct(&f.FlowCtx, ps.Core.Distinct, inputs[0], outputs[0])
	}
	if ps.Core.Aggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newAggregator(&f.FlowCtx, ps.Core.Aggregator, inputs[0], outputs[0])
	}
	if ps.Core.MergeJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newMergeJoiner(&f.FlowCtx, ps.Core.MergeJoiner, inputs, outputs[0])
	}
	return nil, errors.Errorf("unsupported processor %s", ps)
}

func (f *Flow) setupFlow(spec *FlowSpec) error {
	// First step: setup the input synchronizers for all processors.
	inputSyncs := make([][]RowSource, len(spec.Processors))
	for pIdx, ps := range spec.Processors {
		for _, is := range ps.Input {
			if len(is.Streams) == 0 {
				return errors.Errorf("input sync with no streams")
			}
			var sync RowSource
			switch is.Type {
			case InputSyncSpec_UNORDERED:
				if len(is.Streams) == 1 {
					rowChan := &RowChannel{}
					rowChan.Init()
					if err := f.setupInboundStream(is.Streams[0], rowChan); err != nil {
						return err
					}
					sync = rowChan
				} else {
					mrc := &MultiplexedRowChannel{}
					mrc.Init(len(is.Streams))
					for _, s := range is.Streams {
						if err := f.setupInboundStream(s, mrc); err != nil {
							return err
						}
					}
					sync = mrc
				}
			case InputSyncSpec_ORDERED:
				// Ordered synchronizer: create a RowChannel for each input.
				streams := make([]RowSource, len(is.Streams))
				for i, s := range is.Streams {
					rowChan := &RowChannel{}
					rowChan.Init()
					if err := f.setupInboundStream(s, rowChan); err != nil {
						return err
					}
					streams[i] = rowChan
				}
				var err error
				sync, err = makeOrderedSync(convertToColumnOrdering(is.Ordering), streams)
				if err != nil {
					return err
				}

			default:
				return errors.Errorf("unsupported input sync type %s", is.Type)
			}
			inputSyncs[pIdx] = append(inputSyncs[pIdx], sync)
		}
	}

	f.processors = make([]processor, len(spec.Processors))

	for i := range spec.Processors {
		var err error
		f.processors[i], err = f.makeProcessor(&spec.Processors[i], inputSyncs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Start starts the flow (each processor runs in their own goroutine).
func (f *Flow) Start(doneFn func()) {
	f.doneFn = doneFn
	log.VEventf(
		f.Context, 1, "starting (%d processors, %d outboxes)", len(f.outboxes), len(f.processors),
	)
	f.status = FlowRunning
	f.flowRegistry.RegisterFlow(f.id, f, f.inboundStreams)
	f.waitGroup.Add(len(f.inboundStreams))
	f.waitGroup.Add(len(f.outboxes))
	f.waitGroup.Add(len(f.processors))
	for _, o := range f.outboxes {
		o.start(&f.waitGroup)
	}
	for _, p := range f.processors {
		go p.Run(&f.waitGroup)
	}
}

// Wait waits for all the goroutines for this flow to exit.
func (f *Flow) Wait() {
	f.waitGroup.Wait()
}

// Cleanup should be called when the flow completes (after all processors and
// mailboxes exited).
func (f *Flow) Cleanup() {
	if f.status == FlowFinished {
		panic("flow cleanup called twice")
	}
	if log.V(1) {
		log.Infof(f.Context, "cleaning up")
	}
	sp := opentracing.SpanFromContext(f.Context)
	sp.Finish()
	if f.status != FlowNotStarted {
		f.flowRegistry.UnregisterFlow(f.id)
	}
	f.status = FlowFinished
	f.doneFn()
	f.doneFn = nil
}

// RunSync runs the processors in the flow in order (serially), in the same
// context (no goroutines are spawned).
func (f *Flow) RunSync() {
	for _, p := range f.processors {
		p.Run(nil)
	}
	f.Cleanup()
}
