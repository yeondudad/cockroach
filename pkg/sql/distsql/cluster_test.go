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

package distsql

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	opentracing "github.com/opentracing/opentracing-go"
)

func TestClusterFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numRows = 100

	args := base.TestClusterArgs{ReplicationMode: base.ReplicationManual}
	tc := serverutils.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop()

	sumDigitsFn := func(row int) parser.Datum {
		sum := 0
		for row > 0 {
			sum += row % 10
			row /= 10
		}
		return parser.NewDInt(parser.DInt(sum))
	}

	sqlutils.CreateTable(t, tc.ServerConn(0), "t",
		"num INT PRIMARY KEY, digitsum INT, numstr STRING, INDEX s (digitsum)",
		numRows,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sumDigitsFn, sqlutils.RowEnglishFn))

	kvDB := tc.Server(0).KVClient().(*client.DB)
	desc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	makeIndexSpan := func(start, end int) TableReaderSpan {
		var span roachpb.Span
		prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(desc, desc.Indexes[0].ID))
		span.Key = append(prefix, encoding.EncodeVarintAscending(nil, int64(start))...)
		span.EndKey = append(span.EndKey, prefix...)
		span.EndKey = append(span.EndKey, encoding.EncodeVarintAscending(nil, int64(end))...)
		return TableReaderSpan{Span: span}
	}

	// Set up table readers on three hosts feeding data into a join reader on
	// the third host. This is a basic test for the distributed flow
	// infrastructure, including local and remote streams.
	//
	// Note that the ranges won't necessarily be local to the table readers, but
	// that doesn't matter for the purposes of this test.

	// Start a span (useful to look at spans using Lighstep).
	sp, err := tracing.JoinOrNew(tracing.NewTracer(), nil, "cluster test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := opentracing.ContextWithSpan(context.Background(), sp)
	defer sp.Finish()

	tr1 := TableReaderSpec{
		Table:         *desc,
		IndexIdx:      1,
		OutputColumns: []uint32{0, 1},
		Spans:         []TableReaderSpan{makeIndexSpan(0, 8)},
	}

	tr2 := TableReaderSpec{
		Table:         *desc,
		IndexIdx:      1,
		OutputColumns: []uint32{0, 1},
		Spans:         []TableReaderSpan{makeIndexSpan(8, 12)},
	}

	tr3 := TableReaderSpec{
		Table:         *desc,
		IndexIdx:      1,
		OutputColumns: []uint32{0, 1},
		Spans:         []TableReaderSpan{makeIndexSpan(12, 100)},
	}

	jr := JoinReaderSpec{
		Table:         *desc,
		OutputColumns: []uint32{2},
	}

	txn := client.NewTxn(ctx, *kvDB)
	fid := FlowID{uuid.MakeV4()}

	req1 := &SetupFlowRequest{Txn: txn.Proto}
	req1.Flow = FlowSpec{
		FlowID: fid,
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr1},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_MIRROR,
				Streams: []StreamEndpointSpec{
					{StreamID: 0, Mailbox: &MailboxSpec{TargetAddr: tc.Server(2).ServingAddr()}},
				},
			}},
		}},
	}

	req2 := &SetupFlowRequest{Txn: txn.Proto}
	req2.Flow = FlowSpec{
		FlowID: fid,
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr2},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_MIRROR,
				Streams: []StreamEndpointSpec{
					{StreamID: 1, Mailbox: &MailboxSpec{TargetAddr: tc.Server(2).ServingAddr()}},
				},
			}},
		}},
	}

	req3 := &SetupFlowRequest{Txn: txn.Proto}
	req3.Flow = FlowSpec{
		FlowID: fid,
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &tr3},
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_MIRROR,
					Streams: []StreamEndpointSpec{
						{StreamID: StreamID(2)},
					},
				}},
			},
			{
				Input: []InputSyncSpec{{
					Type:     InputSyncSpec_ORDERED,
					Ordering: Ordering{Columns: []Ordering_Column{{1, Ordering_Column_ASC}}},
					Streams: []StreamEndpointSpec{
						{StreamID: 0, Mailbox: &MailboxSpec{}},
						{StreamID: 1, Mailbox: &MailboxSpec{}},
						{StreamID: StreamID(2)},
					},
				}},
				Core: ProcessorCoreUnion{JoinReader: &jr},
				Output: []OutputRouterSpec{{
					Type:    OutputRouterSpec_MIRROR,
					Streams: []StreamEndpointSpec{{Mailbox: &MailboxSpec{SimpleResponse: true}}},
				}}},
		},
	}

	if err := SetFlowRequestTrace(ctx, req1); err != nil {
		t.Fatal(err)
	}
	if err := SetFlowRequestTrace(ctx, req2); err != nil {
		t.Fatal(err)
	}
	if err := SetFlowRequestTrace(ctx, req3); err != nil {
		t.Fatal(err)
	}

	var clients []DistSQLClient
	for i := 0; i < 3; i++ {
		s := tc.Server(i)
		conn, err := s.RPCContext().GRPCDial(s.ServingAddr())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, NewDistSQLClient(conn))
	}

	if log.V(1) {
		log.Infof(ctx, "Setting up flow on 0")
	}
	if resp, err := clients[0].SetupFlow(ctx, req1); err != nil {
		t.Fatal(err)
	} else if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	if log.V(1) {
		log.Infof(ctx, "Setting up flow on 1")
	}
	if resp, err := clients[1].SetupFlow(ctx, req2); err != nil {
		t.Fatal(err)
	} else if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	if log.V(1) {
		log.Infof(ctx, "Running flow on 2")
	}
	stream, err := clients[2].RunSimpleFlow(ctx, req3)
	if err != nil {
		t.Fatal(err)
	}

	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(msg)
		if err != nil {
			t.Fatal(err)
		}
		rows = testGetDecodedRows(t, &decoder, rows)
	}
	if done, trailerErr := decoder.IsDone(); !done {
		t.Fatal("stream not done")
	} else if trailerErr != nil {
		t.Fatal("error in the stream trailer:", trailerErr)
	}
	// The result should be all the numbers in string form, ordered by the
	// digit sum (and then by number).
	var results []string
	for sum := 1; sum <= 50; sum++ {
		for i := 1; i <= numRows; i++ {
			if int(*sumDigitsFn(i).(*parser.DInt)) == sum {
				results = append(results, fmt.Sprintf("['%s']", sqlutils.IntToEnglish(i)))
			}
		}
	}
	expected := strings.Join(results, " ")
	expected = "[" + expected + "]"
	if rowStr := rows.String(); rowStr != expected {
		t.Errorf("Result: %s\n Expected: %s\n", rowStr, expected)
	}
}
