package org.dataone.cn.batch.logging.type;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.TypeFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReadEventCounterCacheTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

//    @Test
    public void testReadEventCache() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public void testSize() {
        ReadEventCounterCache rec = new ReadEventCounterCache(30);
        assertEquals("Initial Cache size should be 0", 0, rec.size());
        
        // add a few readEvents
        DateTime now = new DateTime();
        System.out.printf("Current time is: %s\n", now.toString());
        rec.putEvent("abcdef", now.minusSeconds(10));
        rec.putEvent("abcdef", now.minusSeconds(0));
        rec.putEvent("xyz", now.minusSeconds(5));
        rec.putEvent("mnop", now.minusSeconds(0));
        
        assertEquals("Cache should have 3 items", 3, rec.size());
    }

    @Test
    public void testContains() {
        ReadEventCounterCache rec = new ReadEventCounterCache(30);
        assertFalse("New cache should be empty",rec.contains("foo"));
        String event = "2348sdlkjbvodi";
        rec.putEvent(event, new DateTime());
        assertTrue("Should contain the new event", rec.contains(event));
    }

    @Test
    public void testPutEvent_Null_DateTimeShouldThrowNPE() {
        try {
            ReadEventCounterCache rec = new ReadEventCounterCache(30);
            rec.putEvent("foo", null);
            fail("putEvent should throw NPE if DateTime is null");
        } catch (NullPointerException e) {
            ;
        }
    }
    
    @Test
    public void testPutEvent_NewDateShouldNotIncreaseSize() {
        ReadEventCounterCache rec = new ReadEventCounterCache(30);
        rec.putEvent("abcd", DateTime.now());
        rec.putEvent("abcd", DateTime.now().plusSeconds(3));
        rec.putEvent("abcd", DateTime.now().plusSeconds(5));
        assertEquals("updates should not increase size of cache", 1, rec.size());
        
    }

    @Test
    public void testPurgeOutdatedCacheEntries() throws InterruptedException {
        ReadEventCounterCache rec = new ReadEventCounterCache(3);
        DateTime now = DateTime.now();
        rec.putEvent("abcd", now.minusSeconds(4));
        rec.putEvent("efg", now.minusSeconds(4));
        rec.putEvent("hijk", now.minusSeconds(4));
        rec.putEvent("lmnop", now.minusSeconds(4));
        rec.pruneOutdatedCacheEntries();
        assertEquals("purging should not remove any entries based on time elapsed", 4, rec.size());
        rec.putEvent("qrs", now);
        assertEquals("The read cache should have 1 entries", 1, rec.size());
    }
    
    @Test 
    public void testIsRepeatVisit() {
        ReadEventCounterCache cache = new ReadEventCounterCache(20);
        DateTime now = DateTime.now();
        cache.putEvent("foo", now);
        cache.putEvent("bar", now.plusSeconds(10));
        assertTrue("repeatVisit should be true", cache.isRepeatVisit("foo", now.plusSeconds(15)));
        assertFalse("repeatVisit should be false", cache.isRepeatVisit("foo",now.plusSeconds(50)));
    }
    
    @Test
    public void testIsLaterOrSameAsLatestCachedDateTime() {
        ReadEventCounterCache cache = new ReadEventCounterCache(30);
        DateTime tZero = DateTime.now();
        cache.putEvent("foo", tZero);
        assertTrue("Same time should return true",cache.isLaterOrSameAsLatestCachedTime(tZero));
        assertTrue("Later time should return true",cache.isLaterOrSameAsLatestCachedTime(tZero.plusSeconds(1)));
        assertFalse("Earlier time should return false",cache.isLaterOrSameAsLatestCachedTime(tZero.minusSeconds(1)));
    }
    
    
    @Test
    public void performanceReadEventCounterCache() throws IOException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");


        InputStream is = this.getClass().getResourceAsStream("parsedLogsCombined.tab");
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
       
        int count = 0;
        ReadEventCounterCache cache = new ReadEventCounterCache();
        int cacheSize = 0;
        
        try {
            long start = (new Date()).getTime();
            String line = r.readLine();
            while (line != null) {
                String[] fields = line.split("\\t");
                
                if (fields.length < 2) continue;
                
                String eventId = fields[0];
                DateTime logDate = formatter.parseDateTime(fields[1]);
                
                if(!cache.isLaterOrSameAsLatestCachedTime(logDate)) {
                    fail("Lines should be added chronologically!!");
                }
                boolean isRepeatVisit = cache.isRepeatVisit(eventId, logDate);
                cache.putEvent(eventId, logDate);
                System.out.printf("         cache size: %d \n",cache.size());
                
                line = r.readLine();
                System.out.println("Count: " + ++count);
            }
            long end = (new Date()).getTime();
            System.out.println("Done!!" + (end - start));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            is.close();
            r.close();
        }
    }
    
    @Test
    public void performanceOriginal() throws IOException {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");

        InputStream is = this.getClass().getResourceAsStream("parsedLogsCombined.tab");
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
       
        HashMap<String, DateTime> readEventCache = new HashMap<String, DateTime>();
        
        int count = 0;
        int cacheSize = 0;
        int readEventCacheCurrentMax = 400;
        NodeReference d1NodeReference = TypeFactory.buildNodeReference("urn:node:bingo");
        
        try {
            long start = (new Date()).getTime();
            String line = r.readLine();
            while (line != null) {
                String[] fields = line.split("\\t");
                
                if (fields.length < 2) continue;
                
                String eventId = fields[0];
                DateTime logDate = formatter.parseDateTime(fields[1]);
                
                readEventCache.put(eventId, logDate);
                System.out.printf("         cache size: %d \n",readEventCache.size());
                
                Date mostRecentLoggedDate = new Date(0);
                int readEventCacheMax = 400;
                
                final Logger logger = Logger.getLogger(ReadEventCounterCache.class.getName());
                if (readEventCache.size() > readEventCacheCurrentMax) {
//                    logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " Purging Read Event Cache, size: " + readEventCache.size());
                    Iterator<Map.Entry<String, DateTime>> iterator = readEventCache.entrySet().iterator();
                    DateTime eventWindowStart = new DateTime(mostRecentLoggedDate).minusSeconds(30 + 1);
                    while (iterator.hasNext()) {
                        Map.Entry<String, DateTime> readEvent = iterator.next();
  //                      System.out.print(".");
                        if (readEvent.getValue().isBefore(eventWindowStart)) {
                            iterator.remove();
                        }
                    }
//                    logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + " Read Event Cache size after purge: " + readEventCache.size());

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
//                            logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + "Adjusting readEventCache max to: " + readEventCacheCurrentMax);
                        } else {
                            readEventCacheCurrentMax = readEventCacheMax;
//                            logger.debug("LogHarvesterTask-" + d1NodeReference.getValue() + "Can't adjust readEventCache max to greater than: " + readEventCacheMax);
                        }
                    }
                }
                
                line = r.readLine();
                System.out.println("Count: " + ++count);
            }
            long end = (new Date()).getTime();
            System.out.println("Done!!" + (end - start));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            is.close();
            r.close();
        }
        
        
        
    }
    

}
