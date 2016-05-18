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

package distsql

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"golang.org/x/net/context"
)

// When guessing lease holders, we try to guess the same node for all the ranges
// applicable, until we hit this limit. The rationale is that maybe a bunch of
// those ranges don't have an active lease, so our guess is going to be
// self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// LeaseHolderResolver resolves key spans to the lease holder of their
// respective ranges. Used when planning physical execution of distributed SQL
// queries.
//
// The LeaseHolderResolver shares the LeaseHolderCache and the
// RangeDescriptorCache with the DistSender.
// TODO(andrei): investigate refactoring the DistSender to use this same variant
// of this API for splitting up KV batches.
//
// All public methods are thread-safe.
type LeaseHolderResolver struct {
	leaseHolderCache *kv.LeaseHolderCache
	rangeCache       *kv.RangeDescriptorCache
	gossip           *gossip.Gossip
	distSender       *kv.DistSender
	stopper          *stop.Stopper
	oracle           leaseHolderOracle

	// nodeDesc is the descriptor of the current node. It might be used to give
	// preference to the current node and others "close" to it when guessing lease
	// holders.
	// TODO(andrei): can the descriptor change at runtime?
	nodeDesc roachpb.NodeDescriptor
}

// LeaseHolderGuessingPolicy enumerates the implementors of leaseHolderOracle.
type LeaseHolderGuessingPolicy byte

const (
	// RandomLHGuessingPolicy guesses randomly.
	RandomLHGuessingPolicy = iota
	// BinPackingLHGuessingPolicy bin-packs the guesses.
	BinPackingLHGuessingPolicy
)

// NewLeaseHolderResolver creates a new LeaseHolderResolver.
func NewLeaseHolderResolver(
	distSender *kv.DistSender,
	gossip *gossip.Gossip,
	nodeDesc roachpb.NodeDescriptor,
	stopper *stop.Stopper,
	guessingPolicy LeaseHolderGuessingPolicy,
) *LeaseHolderResolver {
	var oracle leaseHolderOracle
	switch guessingPolicy {
	case RandomLHGuessingPolicy:
		oracle = &randomOracle{}
	case BinPackingLHGuessingPolicy:
		oracle = &binPackingOracle{
			// This number is based on nothing.
			maxPreferredRangesPerLeaseHolder: 10,
			gossip:   gossip,
			nodeDesc: nodeDesc,
		}
	}
	return &LeaseHolderResolver{
		distSender:       distSender,
		leaseHolderCache: distSender.GetLeaseHolderCache(),
		rangeCache:       distSender.GetRangeDescriptorCache(),
		oracle:           oracle,
		gossip:           gossip,
		stopper:          stopper,
		nodeDesc:         nodeDesc,
	}
}

type descWithEvictionToken struct {
	*roachpb.RangeDescriptor
	evictToken *kv.EvictionToken
}

// ResolveLeaseHolders takes a list of spans and returns a list of lease
// holders, one for every range that overlaps the spans.
// The spans need to be disjoint; they also need to be sorted so that the
// prefetching in the range descriptor cache helps us.
// !!! maybe I need to accept reverse spans for some reason? Look again at what
// the deal with "inclusive" is - some functions in DistSender take inclusive as
// an arg. Why?
func (lr *LeaseHolderResolver) ResolveLeaseHolders(
	ctx context.Context, spans []roachpb.Span,
) ([][]kv.ReplicaInfo, error) {
	leaseHolders := make([][]kv.ReplicaInfo, len(spans))
	descsWithEvictToks, err := lr.getRangeDescriptors(ctx, spans)
	if err != nil {
		return nil, err
	}
	{
		firstSpansDescs := descsWithEvictToks[0]
		descs := make([]*roachpb.RangeDescriptor, 0, 10)
		for i, descTok := range firstSpansDescs {
			if i == 10 {
				break
			}
			descs = append(descs, descTok.RangeDescriptor)
		}
		log.VTracef(2, ctx, "resolved span %s to range descriptors "+
			"(only showing first 10): %+v", spans[0], descs)
	}

	// Keep track of how many ranges we assigned to each node; the
	// leaseHolderOracle can use this to coalesce guesses.
	rangesPerLeaseHolder := make(map[roachpb.NodeID]uint)
	for i := range spans {
		spanDescs := descsWithEvictToks[i]
		for _, descWithTok := range spanDescs {
			var leaseReplicaInfo kv.ReplicaInfo
			if leaseReplicaDesc, ok := lr.leaseHolderCache.Lookup(descWithTok.RangeID); ok {
				// Lease-holder cache hit.
				leaseReplicaInfo.ReplicaDescriptor = leaseReplicaDesc
			} else {
				// Lease-holder cache miss. We'll guess a lease holder.
				leaseHolder, err := lr.oracle.GuessLeaseHolder(
					descWithTok.RangeDescriptor, rangesPerLeaseHolder)
				if err != nil {
					return nil, err
				}
				leaseReplicaInfo.ReplicaDescriptor = leaseHolder
				/* !!!
				// Populate the cache with the correct lease holder. As a byproduct,
				// also try to elect the replica guessed above to actually become the
				// lease holder. Doing this here, early, benefits the command that we'll
				// surely be sending to this presumed lease holder later. It also helps
				// if the same query is repeated in the future.
				// TODO(andrei): figure out the context to pass here. It can't use the
				// current span. Should it be the Server's context for background
				// operations? Or that + a new root span?
				lr.stopper.RunWorker(func() {
					lr.writeLeaseHolderToCache(
						context.TODO(), descWithTok.RangeDescriptor,
						descWithTok.evictToken, replicas)
				})
				*/
			}
			// Fill in the node descriptor.
			nd, err := lr.gossip.GetNodeDescriptor(leaseReplicaInfo.NodeID)
			if err != nil {
				return nil, sqlbase.NewRangeUnavailableError(
					descWithTok.RangeID, leaseReplicaInfo.NodeID)
			}
			leaseReplicaInfo.NodeDesc = nd
			leaseHolders[i] = append(leaseHolders[i], leaseReplicaInfo)
			rangesPerLeaseHolder[leaseReplicaInfo.NodeID]++
		}
	}
	return leaseHolders, nil
}

// leaseHolderOracle is used to guess the lease holder for ranges. This
// interface was extracted so we can experiment with different guessing
// policies.
// Note that guesses can act as self-fulfilling prophecies - if there's no
// active lease, the node that will be asked to execute part of the query (the
// guessed node) will acquire a new lease.
type leaseHolderOracle interface {
	// GuessLeaseHolder returns a guess for one range. Implementors are free to
	// use the rangesPerLeaseHolder param, which has info about the number of
	// ranges already handled by each node for the current SQL query. The map is
	// not updated with the result of this method; the caller is in charge of
	// that.
	GuessLeaseHolder(
		desc *roachpb.RangeDescriptor, rangesPerLeaseHolder map[roachpb.NodeID]uint,
	) (roachpb.ReplicaDescriptor, error)
}

// randomOracle is a leaseHolderOracle that guesses the lease holder randomly
// among the replicas in a range descriptor.
// TODO(andrei): consider implementing also an oracle that prefers the "closest"
// replica.
type randomOracle struct {
}

var _ leaseHolderOracle = &randomOracle{}

func (o *randomOracle) GuessLeaseHolder(
	desc *roachpb.RangeDescriptor, _ map[roachpb.NodeID]uint,
) (roachpb.ReplicaDescriptor, error) {
	return desc.Replicas[rand.Intn(len(desc.Replicas))], nil
}

// binPackingOracle guesses a lease holder for a given range by giving
// preference to replicas that are "close" to the current node. It also tries to
// coalesce guesses together, so it gives preference to replicas on nodes that
// are already assumed to be lease holders for some other ranges that are going
// to be part of a single query. Finally, it tries not to overload any node.
type binPackingOracle struct {
	maxPreferredRangesPerLeaseHolder uint
	gossip                           *gossip.Gossip
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

var _ leaseHolderOracle = &binPackingOracle{}

func (o *binPackingOracle) GuessLeaseHolder(
	desc *roachpb.RangeDescriptor, rangesPerLeaseHolder map[roachpb.NodeID]uint,
) (roachpb.ReplicaDescriptor, error) {
	// Look for a replica that has been assigned some ranges, but it's not yet full.
	for _, repDesc := range desc.Replicas {
		assignedRanges := rangesPerLeaseHolder[repDesc.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repDesc, nil
		}
	}

	// Either no replica was assigned any previous ranges, or all replicas are
	// full. Go through replicas ordered by preference, and pick the least loaded
	// one.

	replicas := kv.NewReplicaSlice(o.gossip, desc)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range desc.Replicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return roachpb.ReplicaDescriptor{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, nodeIDs...)

	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc)

	var bestLeaseHolderIdx int
	minLoad := uint(math.MaxUint32)
	for i, replicaDesc := range replicas {
		assignedRanges := rangesPerLeaseHolder[replicaDesc.NodeID]
		if assignedRanges < minLoad {
			bestLeaseHolderIdx = i
			minLoad = assignedRanges
		}
	}
	return replicas[bestLeaseHolderIdx].ReplicaDescriptor, nil
}

/* !!!
// writeLeaseHolderToCache resolves the lease holder of a range by probing the
// replicas in a given order. On success, the lease holder is stored in the
// LeaseHolderCache. Failures are swallowed.
// Probing a replica also the effect that the replica tries to acquire a lease.
// So if there's no active lease for the respective range, this will cause the
// recipient to become the lease holder. In other words, be careful whom you put
// at the head of the replicas list.
func (lr *LeaseHolderResolver) writeLeaseHolderToCache(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	evictionToken *kv.EvictionToken,
	replicas kv.ReplicaSlice,
) {
	l, err := lr.distSender.FindLeaseHolder(
		ctx, desc, evictionToken, replicas)
	if err != nil {
		log.VTracef(1, ctx, "failed to find lease holder: %s", err)
	}
	log.Infof(ctx, "!!! updated cache for range: %s to: %s", desc.RSpan(), l.Replica)
	// TODO(andrei): at this point we know the real lease holder. If this doesn't
	// correspond to the guess we've already returned to the client, we could
	// have some channel for informing of this if it's not too late.
}
*/

// getRangeDescriptors takes a list of spans are resolves it to a list of
// ranges. Spans need to be disjoint and sorted, and so will the results be.
func (lr *LeaseHolderResolver) getRangeDescriptors(
	ctx context.Context, spans []roachpb.Span,
) (map[int][]descWithEvictionToken, error) {
	res := make(map[int][]descWithEvictionToken)
	for i, span := range spans {
		descsWithEvictToks, err := lr.resolveSpan(ctx, span)
		if err != nil {
			return nil, err
		}
		log.VTracef(2, ctx, "resolved span %q to: %+v", span, descsWithEvictToks)
		res[i] = descsWithEvictToks
	}
	return res, nil
}

func (lr *LeaseHolderResolver) resolveSpan(
	ctx context.Context, span roachpb.Span,
) ([]descWithEvictionToken, error) {
	var retryOptions retry.Options
	// !!! init the options

	var res []descWithEvictionToken
	needAnother := true
	for needAnother {
		var desc *roachpb.RangeDescriptor
		var evictToken *kv.EvictionToken
		var err error
		for r := retry.Start(retryOptions); r.Next(); {
			log.Trace(ctx, "meta descriptor lookup")
			startKey, err := keys.Addr(span.Key)
			if err != nil {
				return nil, err
			}
			var endKey roachpb.RKey
			if len(span.EndKey) != 0 {
				var err error
				endKey, err = keys.Addr(span.EndKey)
				if err != nil {
					return nil, err
				}
			}
			rs := roachpb.RSpan{Key: startKey, EndKey: endKey}
			desc, needAnother, evictToken, err = kv.ResolveKeySpanToFirstDescriptor(
				ctx, lr.rangeCache, rs, evictToken, false /* isReverse */)
			// We assume that all errors coming from ResolveKeySpanToFirstDescriptor
			// are retryable, as per its documentation.
			if err != nil {
				log.VTracef(1, ctx, "range descriptor lookup failed: %s", err.Error())
				continue
			} else {
				log.VTracef(2, ctx, "looked up range descriptor")
				res = append(res, descWithEvictionToken{
					RangeDescriptor: desc, evictToken: evictToken})
				break
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
