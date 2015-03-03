/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.batch.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.exceptions.ServiceFailure;

/**
 * 
 * Keep a pool of NodeRegistryService instances. One for each node that is accessed by log aggregation.
 * 
 * @author waltz
 */
public class NodeRegistryPool {
    private static ConcurrentMap<String, NodeRegistryService> initializedNodeRegistry =  new ConcurrentHashMap<String, NodeRegistryService>();
    private static NodeRegistryPool nodeRegistryPool;
    private NodeRegistryPool() {
    }

    public static NodeRegistryPool getInstance() {
        if (nodeRegistryPool == null) {
            nodeRegistryPool = new NodeRegistryPool();
        }
        return nodeRegistryPool;
    }
    public NodeRegistryService getNodeRegistryService(String nodeId) throws ServiceFailure {

        if (initializedNodeRegistry.containsKey(nodeId)) {
            return initializedNodeRegistry.get(nodeId);
        } else {

            NodeRegistryService nodeRegistryService = new NodeRegistryService();

            initializedNodeRegistry.putIfAbsent(nodeId, nodeRegistryService);
            return nodeRegistryService;
        }
    }
}
