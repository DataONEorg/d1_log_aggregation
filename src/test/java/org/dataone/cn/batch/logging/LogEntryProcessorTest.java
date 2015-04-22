
/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id#
 */

package org.dataone.cn.batch.logging;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.cn.batch.logging.tasks.LogAggregatorTask;
import org.dataone.cn.batch.logging.type.LogEntrySolrItem;
import org.joda.time.DateTime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;

import org.apache.commons.csv.*;
import org.apache.log4j.Logger;

/* Test processing/preparation of DataONE event log entries. Currently it's not
 * possible to write a unit test for higher level classes such as
 * LogAggregatorTask, as this would require a mock solr server and has dependencies
 * on Quartz scheduler, so this test simply checks the index entry preparation
 * calls that LogAggregatorTask calls, to ensure that log entries are
 * processed correctly before being added to the event index. */
public class LogEntryProcessorTest extends TestCase {


	/**
	 * Constructor to build the test
	 * 
	 * @param name
	 *            the name of the test method
	 */
	public LogEntryProcessorTest(String name) {
		super(name);
	}

	/**
	 * Establish a testing framework by initializing appropriate objects
	 */
	public void setUp() throws Exception {
	}

	/**
	 * Release any objects after tests are complete
	 */
	public void tearDown() {
	}

	/**
	 * Create a suite of tests to be run together
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite();
		//suite.addTest(new GeoIPtest("initialize"));
		suite.addTest(new LogEntryProcessorTest("testRobotFilter"));

		return suite;
	}

	/**
	 * Test the method setCOUNTERFields to ensure that it is flagging read requests
	 * that are repeat visits or requests that were made by web robots.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public void testRobotFilter() throws Exception {

        Logger logger = Logger.getLogger(LogAggregatorTask.class.getName());
        String filePath = "webRobotIPs.csv";
        List<CSVRecord> logRecords;
        BufferedReader inBuf;
        CSVParser parser;
        // Set the nodeId to any value for testing
        String nodeIdStr = "urn:node:mnTestKNB";
        NodeReference nodeIdentifier = new NodeReference();
        nodeIdentifier.setValue(nodeIdStr);
        
        // Event log record types to check for counter compliance
        HashSet<String> eventsToCheck = new HashSet<String>(Arrays.asList("read"));
        // List of web robots according to COUNTER standard
        ArrayList<String> fullWebRobotList = new ArrayList<String>();
        // Less strict list of web robots than COUNTER standard
        ArrayList<String> partialWebRobotList = new ArrayList<String>();
        // Cache for the read events, indexed by IPaddress. This cache will grow as the log entries
        // are processed, so it will be purged after it reaches a certain size.
        HashMap<String, DateTime> readEventCache = new HashMap<String, DateTime>();
        // Initial max number of read events that can be in the cache, can increase to readEventCacheMax
        int readEventCacheCurrentMax = 20;
        int readEventCacheMax = 50;
        
        List<CSVRecord> webRobotIPs = new ArrayList<CSVRecord>();
        
        String LogEntryDataFile = "./src/test/resources/org/dataone/cn/batch/logging/LogEntries.csv";
        String fullWebRobotListFilePath = "./src/test/resources/org/dataone/cn/batch/logging/fullWebRobotList.txt";
        String parialWebRobotListFilePath = "./src/test/resources/org/dataone/cn/batch/logging/partialWebRobotList.txt";
        String webRobotIPsFilePath = "./src/test/resources/org/dataone/cn/batch/logging/webRobotIPs.csv";
        String DataONE_IPsFilePath = "./src/test/resources/org/dataone/cn/batch/logging/DataONE_IPs.csv";
        final int repeatVisitIntervalSeconds = 30;
        // In production code, this will be a property read in from the log agg properties file.
        Boolean doWebRobotIPcheck = true;
        
		if (doWebRobotIPcheck) {
			// Read in the list of web robot IP addresses
			try {
				// Read in the "black list" of IPs from sites external to DataONE
				inBuf = new BufferedReader(new FileReader(webRobotIPsFilePath));
				parser = new CSVParser(inBuf, CSVFormat.RFC4180);
				webRobotIPs = parser.getRecords();
				parser.close();
				// Add the list of DataONE CNs and MNs
				inBuf = new BufferedReader(new FileReader(DataONE_IPsFilePath));
				parser = new CSVParser(inBuf, CSVFormat.RFC4180);
				webRobotIPs.addAll(parser.getRecords());
				parser.close();
			} catch (FileNotFoundException ex) {
				System.out.println("Error: can't find file");
				throw (ex);
			} catch (IOException ex) {
				System.out.println("Error: can't read file");
				throw (ex);
			}
		}
        
		try {
            String inLine;
            // Read in the "full" list of web robots
			filePath = fullWebRobotListFilePath;
			inBuf = new BufferedReader(new FileReader(filePath));
			while ((inLine = inBuf.readLine()) != null) {
				fullWebRobotList.add(inLine);
			}
			inBuf.close();
	        // Read in the "partial" list of web robots
			filePath = parialWebRobotListFilePath;
			inBuf = new BufferedReader(new FileReader(filePath));
			while ((inLine = inBuf.readLine()) != null) {
				partialWebRobotList.add(inLine);
			}
			inBuf.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
			logger.error("LogAggregatorTask-"
					+ ex.getMessage()
					+ String.format("Unable to open file '%s' which is needed for COUNTER compliance checking", filePath));
			throw new ExecutionException(ex);
		} catch (IOException ex) {
			ex.printStackTrace();
			logger.error("LogAggregatorTask-"
					+ ex.getMessage()
					+ String.format("Error reading file '%s' which is needed for COUNTER compliance checking", filePath));
			throw new ExecutionException(ex);
		}
		
        // Read in sample MN log entry data records. These were obtained directly from the
		// database table on on metacat member node with the command:
		// psql -U metacat ; \c knb ; COPY access_log TO '/tmp/file.csv' DELIMITER ',' CSV HEADER;

        try {
            inBuf = new BufferedReader(new FileReader(LogEntryDataFile));
            parser = new CSVParser(inBuf, CSVFormat.RFC4180);
            logRecords = parser.getRecords();
        } catch (FileNotFoundException ex) {
            System.out.println("Error: can't find file");
            throw(ex);
        } catch (IOException ex) {
            System.out.println("Error: can't read file");
            throw(ex);
        }
        
        // A log record has the fields: 
        //   entryid (bigint)
        //   ip_address (character)
        //   principal (character)
        //   docid (character)
        //   event (character)
        //   date_logged (timestamp without time zone)
        
        LogEntry logEntry;
        LogEntrySolrItem solrItem;
        int inPartialWebRobotListCount = 0;
        int inFullWebRobotListCount = 0;
        int repeatVisitCount = 0;
        SimpleDateFormat simpleDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date mostRecentLoggedDate = simpleDF.parse("1900-01-01 00:00:00.000");

        for (CSVRecord  logRec : logRecords) {
        	logEntry = new LogEntry();
        	logEntry.setNodeIdentifier(nodeIdentifier);
            logEntry.setEntryId(logRec.get(0));
            //System.out.println("EntryId: " + logEntry.getEntryId());
            logEntry.setIpAddress(logRec.get(1));
            Subject thisSubject = new Subject();
            thisSubject.setValue(logRec.get(2));
            logEntry.setSubject(thisSubject);
            Identifier thisId = new Identifier();
            thisId.setValue(logRec.get(3));
            //System.out.println("id: " + thisId.getValue());
            logEntry.setIdentifier(thisId);
            logEntry.setEvent(Event.convert(logRec.get(4)));
            logEntry.setDateLogged(simpleDF.parse(logRec.get(5)));
            logEntry.setUserAgent(logRec.get(6));
            solrItem = new LogEntrySolrItem(logEntry);
            // This is the core of this unit test, to ensure that this routine sets fields correctly
            // for each entry.
        	solrItem.setCOUNTERfields(partialWebRobotList, fullWebRobotList, readEventCache, eventsToCheck, repeatVisitIntervalSeconds, webRobotIPs, doWebRobotIPcheck);
        	if (solrItem.getInFullRobotList()) {
        		inFullWebRobotListCount += 1;
        	}
        	if (solrItem.getInPartialRobotList()) {
        		inPartialWebRobotListCount += 1;
        	}
        	if (solrItem.getIsRepeatVisit()) {
        		repeatVisitCount += 1;
        	}
            if (logEntry.getDateLogged().after(mostRecentLoggedDate)) {
                mostRecentLoggedDate = logEntry.getDateLogged();
            }
            // Purge the read event cache if it grows past a specified max value, hoever
            // the number of items in the cache is determined by how far away they are from
            // the time of the last event, so the max cache size can grow if needed.
           	if (readEventCache.size() > readEventCacheCurrentMax) {
                //System.out.println(" Purging Read Event Cache. size: " + readEventCache.size());
                Iterator<Map.Entry<String, DateTime>> iterator = readEventCache.entrySet().iterator();
                DateTime eventWindowStart = new DateTime(mostRecentLoggedDate).minusSeconds(repeatVisitIntervalSeconds+1);
                while(iterator.hasNext()){
                    Map.Entry<String, DateTime> readEvent = iterator.next();                                
        			if (readEvent.getValue().isBefore(eventWindowStart)) {
                        iterator.remove();
        			}
                }
                //System.out.println("Read Event Cache size after purge: " + readEventCache.size());
                // The eventCache purged of events that are older than repeatVisitIntervalSeconds minus the latest time.
                // If the cache is larger max size after the purge, then adjust the max size to 5% greater than the
                // purged size.
                // The intent here is to intelligently increase the size of the cache so that it fits the current time window size,
                // so that the cache isn't being purged continually.
                if (readEventCache.size() > readEventCacheCurrentMax) {
                    float perc;
                    perc = (float) readEventCache.size() * (5.0f / 100.0f);
                    int newMax = readEventCache.size() + Math.round(perc);
                    // Try to increase cache size, but don't increase past max value
                    if (newMax < readEventCacheMax) {
                    	readEventCacheCurrentMax = newMax;
                    	//System.out.println("Adjusting readEventCache max to: " + readEventCacheCurrentMax);
                    } else {
                    	readEventCacheCurrentMax = readEventCacheMax;
                    	//System.out.println("Can't adjust readEventCache max to greater than: " + readEventCacheMax);
                    }
                }
        	}
        }
        
		assertEquals(inFullWebRobotListCount, 23, .001);
		assertEquals(inPartialWebRobotListCount, 18, .001);
		assertEquals(repeatVisitCount, 9, .001);
	}
}
