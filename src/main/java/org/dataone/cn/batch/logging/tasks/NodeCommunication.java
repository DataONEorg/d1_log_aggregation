package org.dataone.cn.batch.logging.tasks;

import java.text.SimpleDateFormat;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;

import org.dataone.cn.batch.logging.exceptions.QueryLimitException;
import org.dataone.cn.batch.logging.type.LogQueryDateRange;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v2.LogEntry;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v2.Node;

public abstract class NodeCommunication {

    protected SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    protected Integer batchSize = Settings.getConfiguration().getInt("LogAggregator.logRecords_batch_size", 1000);

    public abstract List<LogEntry> retrieve(NodeReference d1NodeReference, Stack<LogQueryDateRange> logQueryStack, Integer queryTotalLimit) 
    		throws Exception;
    
    
    public static NodeCommunication getInstance(Node targetNode) {
		
		
		// default to v1, but use v2 if we find it enabled
    	NodeCommunication impl = new org.dataone.cn.batch.logging.tasks.v1.MNCommunication();
		
		for (Service service : targetNode.getServices().getServiceList()) {
            if (service.getName().equals("MNCore")
                    && service.getVersion().equals("v2")) {
                impl = new org.dataone.cn.batch.logging.tasks.v2.MNCommunication();
                break;
            }
        }
		
		return impl;
	}
}
