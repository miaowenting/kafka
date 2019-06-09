/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 *
 * InFlightRequests队列的主要作用是缓存了已经发出去但没收到响应的ClientRequest。
 * 其底层是通过一个Map<String, Deque<ClientRequest>> 对象实现的，key 是Nodeld,
 * value是发送到对应Node的ClientRequest对象集合。InFlightRequests 提供了很多管理这
 * 个缓存队列的方法，还通过配置参数，限制了每个连接最多缓存的ClientRequest个数。
 *
 */
final class InFlightRequests {

    private final int maxInFlightRequestsPerConnection;
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    /** Thread safe total number of in flight requests. */
    private final AtomicInteger inFlightRequestCount = new AtomicInteger(0);

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    public void add(NetworkClient.InFlightRequest request) {
        String destination = request.destination;
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(destination, reqs);
        }
        // 增加至对头
        reqs.addFirst(request);
        inFlightRequestCount.incrementAndGet();
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * Get the oldest request (the one that will be completed next) for the given node
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollLast();
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollFirst();
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Can we send more requests to this node?
     *
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     *
     * 队头的请求发送不出去，可能网络出现问题。队头的消息与对应的KafkaChannel.send字段指向的是同一个消息。
     * 还需判断InFlightRequests队列中是否堆积过多请求
     */
    public boolean canSendMore(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        /**
         * queue = null和queue.isEmpty0这两个条件比较容易理解，不再赘述，queue.peekFirst0.
         requsl0.completed0这个条件为true表示当前队头的请求已经发送完成，如果队头的请求迟
         迟发送不出去，可能是网络出现问题，则不能继续向此Node发送请求。此外，队头的消息
         与对应KafkaChannel.send字段指向的是同-一个消息，为了避免未发送的消息被覆盖，也不
         能让KafkaChannel.send字段指向新请求。最后queue.size( < this.maxInFlightRequestsPerConn
         ection)条件则是为了判断InFlightRequests队列中是否堆积过多请求。如果Node已经堆积了
         很多未响应的请求，说明这个节点负载可能较大或是网络连接有问题，继续向其发送请求，
         则可能导致请求超时。
         */
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    public int count() {
        return inFlightRequestCount.get();
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            final Deque<NetworkClient.InFlightRequest> clearedRequests = requests.remove(node);
            inFlightRequestCount.getAndAdd(-clearedRequests.size());
            return () -> clearedRequests.descendingIterator();
        }
    }

    private Boolean hasExpiredRequest(long now, Deque<NetworkClient.InFlightRequest> deque) {
        for (NetworkClient.InFlightRequest request : deque) {
            long timeSinceSend = Math.max(0, now - request.sendTimeMs);
            if (timeSinceSend > request.requestTimeoutMs)
                return true;
        }
        return false;
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @return list of nodes
     */
    public List<String> nodesWithTimedOutRequests(long now) {
        List<String> nodeIds = new ArrayList<>();
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();
            if (hasExpiredRequest(now, deque))
                nodeIds.add(nodeId);
        }
        return nodeIds;
    }

}
