package org.dataone.cn.batch.logging;

import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v2.Node;

public class NodeHarvesterFactory {

    public static NodeHarvester getNodeHarvester(Node targetNode) {

	for (Service service : targetNode.getServices().getServiceList()) {
            if (service.getName().equals("MNCore")
                    && service.getVersion().equals("v2")) {
                return  new org.dataone.cn.batch.logging.v2.MNCommunication(targetNode.getIdentifier());
            }
        }
        return new org.dataone.cn.batch.logging.v1.MNCommunication(targetNode.getIdentifier());
		

	}
}
