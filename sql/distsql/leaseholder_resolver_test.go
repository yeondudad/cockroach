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
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsql_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/distsql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

func TestResolveLeaseHolders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop()

	if _, err := tc.Conns[0].Exec(`CREATE DATABASE t`); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Conns[0].Exec(`CREATE TABLE test (k INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Conns[0].Exec(`INSERT INTO test VALUES (1), (2), (3)`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(tc.Servers[0].DB(), "t", "test")
	log.Infof(context.TODO(), "!!! test about to split")
	// Split every SQL row to its own range.
	// TODO(andrei): use the new SQL SPLIT statement when available, and then we
	// also don't need to actually INSERT any data any more.
	rowRanges := make([]*roachpb.RangeDescriptor, 4)
	for i := 0; i < 3; i++ {
		var err error
		var l *roachpb.RangeDescriptor
		l, rowRanges[i], err = splitRangeAtKey(tc, tableDesc, i)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			rowRanges[i-1] = l
		}
	}
	log.Infof(context.TODO(), "!!! test done splitting")
	// Replicate the row ranges on all of the first 3 nodes. Save the 4th node in
	// a pristine state, with empty caches.
	for i := 0; i < 3; i++ {
		var err error
		rowRanges[i], err = tc.AddReplicas(
			rowRanges[i].StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Infof(context.TODO(), "!!! test done replicating")

	// Scatter the leases around; node i gets range i.
	for i := 0; i < 3; i++ {
		if err := tc.TransferRangeLease(rowRanges[i], tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		// Wait for everybody to apply the new lease, so that we can rely on the
		// lease discovery done later by the LeaseHolderResolver to be up to date.
		util.SucceedsSoon(t, func() error {
			for j := 0; j < 3; j++ {
				target := tc.Target(j)
				rt, err := tc.FindRangeLeaseHolder(rowRanges[i], &target)
				if err != nil {
					return err
				}
				if rt != tc.Target(i) {
					return errors.Errorf("node %d hasn't applied the lease yet", j)
				}
			}
			return nil
		})
	}
	log.Infof(context.TODO(), "!!! test done moving leases")

	// Create a LeaseHolderResolver using the 4th node, with empty caches.
	s3 := tc.Servers[3]
	// Evict the descriptor for range 1 from the cache. It's probably stale since
	// this test has been mocking with that range. And a stale descriptor would
	// affect our resolving.
	rdc := s3.DistSender().GetRangeDescriptorCache()
	err := rdc.EvictCachedRangeDescriptor(roachpb.RKeyMin, nil, false /* inclusive */)
	if err != nil {
		t.Fatal(err)
	}
	// !!! why exactly was this eviction necessary? How come gossip wasn't taking
	// care of it?

	lr := distsql.NewLeaseHolderResolver(
		s3.DistSender(), s3.Gossip(), s3.GetNode().Descriptor, s3.Stopper(),
		distsql.BinPackingLHGuessingPolicy)

	var spans []roachpb.Span
	for i := 0; i < 3; i++ {
		spans = append(
			spans,
			roachpb.Span{Key: rowRanges[i].StartKey.AsRawKey(), EndKey: rowRanges[i].EndKey.AsRawKey()})
	}

	// Resolve the spans. Since the LeaseHolderCache is empty, all the ranges
	// should be grouped and "assigned" to replica 0.
	log.Infof(context.TODO(), "!!! test about to resolve")
	replicas, err := lr.ResolveLeaseHolders(context.TODO(), spans)
	if err != nil {
		t.Fatal(err)
	}
	if len(replicas) != 3 {
		t.Fatalf("expected replies for 3 spans, got %d: %+v", len(replicas), replicas)
	}
	si := tc.Servers[0]
	nodeID := si.GetNode().Descriptor.NodeID
	storeID := si.GetFirstStoreID()
	for i := 0; i < 3; i++ {
		if len(replicas[i]) != 1 {
			t.Fatalf("expected 1 range for span %s, got %d (%+v)",
				len(replicas[i]), replicas[i])
		}
		rd := replicas[i][0].ReplicaDescriptor
		if rd.NodeID != nodeID || rd.StoreID != storeID {
			t.Fatalf("expected span %s to be on replica (%d, %d) but was on %s",
				spans[i], nodeID, storeID, rd)
		}
	}

	/* !!!
	// Check that the LeaseHolderCache updates the cache asynchronously and so
	// soon enough the node in question has up-to-date info.
	util.SucceedsSoon(t, func() error {
		replicas, err = lr.ResolveLeaseHolders(context.TODO(), spans)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if len(replicas[i]) != 1 {
				t.Fatalf("expected 1 range for span %s, got %d (%+v)",
					len(replicas[i]), replicas[i])
			}
			si := tc.Servers[i]
			nodeID := si.GetNode().Descriptor.NodeID
			storeID := si.GetFirstStoreID()
			rd := replicas[i][0].ReplicaDescriptor
			if rd.NodeID != nodeID || rd.StoreID != storeID {
				return errors.Errorf("expected span %s to be on replica (%d, %d) but was on %s",
					spans[i], nodeID, storeID, rd)
			}
		}
		return nil
	})
	*/
}

// splitRangeAtKey splits the range for a table with schema
// `CREATE TABLE test (k INT PRIMARY KEY)` at row with value pk (the row will be
// the first on the right of the split).
func splitRangeAtKey(
	tc *testcluster.TestCluster, tableDesc *sqlbase.TableDescriptor, pk int,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	if len(tableDesc.Columns) != 1 {
		return nil, nil, errors.Errorf("expected table with one col, got: %+v", tableDesc)
	}
	if tableDesc.Columns[0].Type.Kind != sqlbase.ColumnType_INT {
		return nil, nil, errors.Errorf("expected table with one INT col, got: %+v", tableDesc)
	}

	if len(tableDesc.Indexes) != 0 {
		return nil, nil, errors.Errorf("expected table with just a PK, got: %+v", tableDesc)
	}
	if len(tableDesc.PrimaryIndex.ColumnIDs) != 1 ||
		tableDesc.PrimaryIndex.ColumnIDs[0] != tableDesc.Columns[0].ID ||
		tableDesc.PrimaryIndex.ColumnDirections[0] != sqlbase.IndexDescriptor_ASC {
		return nil, nil, errors.Errorf("table with unexpected PK: %+v", tableDesc)
	}

	pik, err := primaryIndexKey(tableDesc, parser.NewDInt(parser.DInt(pk)))
	if err != nil {
		return nil, nil, err
	}
	startKey := keys.MakeFamilyKey(pik, uint32(tableDesc.Families[0].ID))
	leftRange, rightRange, err := tc.SplitRange(startKey)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to split at row: %d", pk)
	}
	return leftRange, rightRange, nil
}

func primaryIndexKey(tableDesc *sqlbase.TableDescriptor, val parser.Datum) (roachpb.Key, error) {
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	colIDtoRowIndex := map[sqlbase.ColumnID]int{tableDesc.Columns[0].ID: 0}
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, []parser.Datum{val}, primaryIndexKeyPrefix)
	if err != nil {
		return nil, err
	}
	return roachpb.Key(primaryIndexKey), nil
}
