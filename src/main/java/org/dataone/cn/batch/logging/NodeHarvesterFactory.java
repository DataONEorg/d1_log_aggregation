package org.dataone.cn.batch.logging;

import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v2.Node;
/*
 Given a Node create a NodeCommunication object that implements NodeHarvester, and return the correct
 version based on NodeType and Services
 1) Coordinating Nodes always return the latest NodeCommunication version
 2) Member Nodes will return a NodeCommunication object based on services offered

 XXX For v2.1 target, May wish to refactor this such that a v1 NodeHarverFactory always return a v1 NodeCommunication object
 while a v2 NodeHarverFactory will determine if a Node has v2 implemented if not then call super.getNodeHarvester
  
 */

public class NodeHarvesterFactory {

    public static NodeHarvester getNodeHarvester(Node targetNode) {

        // Coordinating nodes should aways return the latest Node Communication Version
        if (targetNode.getType().compareTo(NodeType.CN) == 0) {
            return new org.dataone.cn.batch.logging.v2.NodeCommunication(targetNode.getIdentifier());
        }
        if (targetNode.getType().compareTo(NodeType.MN) == 0) {
            for (Service service : targetNode.getServices().getServiceList()) {
                if (service.getName().equals("MNCore")
                        && service.getVersion().equals("v2")) {
                    return new org.dataone.cn.batch.logging.v2.NodeCommunication(targetNode.getIdentifier());
                }
            }
        }
        return new org.dataone.cn.batch.logging.v1.NodeCommunication(targetNode.getIdentifier());

    }
}
