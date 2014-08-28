/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.cluster.routing.allocation.deallocator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.ArrayList;
import java.util.List;


public class PrimariesDeallocator extends AbstractComponent implements Deallocator, ClusterStateListener {

    static final Predicate<MutableShardRouting> ALL_PRIMARY_SHARDS = new Predicate<MutableShardRouting>() {
        @Override
        public boolean apply(@Nullable MutableShardRouting input) {
            return input != null
                    && (input.started() || input.initializing())
                    && input.primary();
        }
    };

    private final ClusterService clusterService;
    private final ClusterInfoService clusterInfoService;
    private final AllocationDeciders allocationDeciders;
    private final TransportClusterRerouteAction clusterRerouteAction;
    private String localNodeId;

    private final Object localNodeFutureLock = new Object();
    private volatile SettableFuture<DeallocationResult> localNodeFuture;

    @Inject
    public PrimariesDeallocator(ClusterService clusterService,
                                ClusterInfoService clusterInfoService,
                                AllocationDeciders allocationDeciders, TransportClusterRerouteAction clusterRerouteAction) {
        super(ImmutableSettings.EMPTY);
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.allocationDeciders = allocationDeciders;
        this.clusterRerouteAction = clusterRerouteAction;
        this.clusterService.add(this);
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }

    @Override
    public ListenableFuture<DeallocationResult> deallocate() {
        if (isDeallocating()) {
            throw new IllegalStateException("node already waiting for primary only deallocation");
        }
        final RoutingNodes routingNodes = clusterService.state().routingNodes();
        final RoutingNode node = routingNodes.node(localNodeId());

        if (node.size() == 0) {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        synchronized (localNodeFutureLock) {
            localNodeFuture = SettableFuture.create();
        }

        // if primary has active replica - do a swap operaion
        // if not move primary to another node
        List<MutableShardRouting> shardsToSwap = new ArrayList<>();
        List<MutableShardRouting> shardsToMove = new ArrayList<>();

        for (MutableShardRouting primary : Iterables.filter(node, ALL_PRIMARY_SHARDS)) {
            MutableShardRouting replica = routingNodes.activeReplica(primary);
            if (replica != null) {
                shardsToSwap.add(primary);
            } else {
                shardsToMove.add(primary);
            }
        }
        if (!shardsToSwap.isEmpty()) {
            swapPrimaries(shardsToSwap);
        }
        if (!shardsToMove.isEmpty()) {
            movePrimaries(node, shardsToMove);
        }
        return localNodeFuture;
    }

    private void movePrimaries(RoutingNode fromNode, List<MutableShardRouting> shardsToMove) {
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        for (MutableShardRouting shard : shardsToMove) {
            RoutingNode moveToNode = getNextAllocatableNode(shard);
            if (moveToNode == null) {
                cancelWithExceptionIfPresent(
                        new DeallocationFailedException(
                                fromNode,
                                shard,
                                "no node available to move to"));
                return;
            }
            request.add(new MoveAllocationCommand(shard.shardId(), fromNode.nodeId(), moveToNode.nodeId()));
        }
        clusterRerouteAction.execute(request, new ActionListener<ClusterRerouteResponse>() {
            @Override
            public void onResponse(ClusterRerouteResponse clusterRerouteResponse) {
                // TODO: check explanations for not moved shards
            }

            @Override
            public void onFailure(Throwable e) {
                cancelWithExceptionIfPresent(e);
            }
        });
    }

    private void swapPrimaries(List<MutableShardRouting> shardsToSwap) {
        clusterService.submitStateUpdateTask("primaries deallocator", new SwapPrimariesTask(shardsToSwap));
    }

    private boolean cancelWithExceptionIfPresent(Throwable e) {
        synchronized (localNodeFutureLock) {
            SettableFuture<DeallocationResult> future = localNodeFuture;
            if (future != null) {
                future.setException(e);
                localNodeFuture = null;
                return true;
            }
        }
        return false;
    }

    private @Nullable RoutingNode getNextAllocatableNode(MutableShardRouting shardRouting) {
        // TODO: get other node with least number of shards
        return null;
    }

    @Override
    public boolean cancel() {
        return cancelWithExceptionIfPresent(new DeallocationCancelledException(localNodeId()));
    }

    @Override
    public boolean isDeallocating() {
        return localNodeFuture != null;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        synchronized (localNodeFutureLock) {
            if (localNodeFuture != null) {
                int primariesLeft = 0;
                for (MutableShardRouting shard : event.state().routingNodes().node(localNodeId())) {
                    if (shard.primary()) {
                        primariesLeft++;
                    }
                }
                if (primariesLeft == 0) {
                    localNodeFuture.set(DeallocationResult.SUCCESS);
                    localNodeFuture = null;
                } else {
                    logger.trace("[{}] {} primaries left", localNodeId(), primariesLeft);
                }
            }
        }
    }

    private class SwapPrimariesTask implements ClusterStateUpdateTask {

        private final List<MutableShardRouting> primaries;

        protected SwapPrimariesTask(List<MutableShardRouting> primaries) {
            this.primaries = primaries;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            boolean changed = false;
            RoutingNodes routingNodes = currentState.routingNodes();
            for (MutableShardRouting primary : primaries) {
                MutableShardRouting replica = routingNodes.activeReplica(primary);
                if (replica == null) {
                    throw new DeallocationFailedException(routingNodes.node(localNodeId()), primary, "no replica found");
                } else {
                    // do the swap
                    routingNodes.swapPrimaryFlag(primary, replica);
                }
            }
            RoutingAllocation.Result result = new RoutingAllocation.Result(changed,
                    RoutingTable.builder()
                            .updateNodes(routingNodes)
                            .build().validateRaiseException(currentState.metaData()));
            return ClusterState.builder(currentState).routingResult(result).build();
        }

        @Override
        public void onFailure(String source, Throwable t) {
            logger.error("[{}] Error swapping primaries", t, localNodeId());
            cancelWithExceptionIfPresent(t);
        }
    }

}
