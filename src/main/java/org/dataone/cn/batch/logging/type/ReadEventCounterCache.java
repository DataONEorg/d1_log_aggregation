package org.dataone.cn.batch.logging.type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;


/**
 * The ReadEventCounterCache is used for detecting "double-clicks" by a user, 
 * as defined by Project Counter (see page 7 of https://www.projectcounter.org/wp-content/uploads/2016/03/Technical-pdf.pdf) 
 * The default constructor uses the Project Counter standard of 30 seconds to 
 * detect double-clicks, but provides a parameterized constructor for flexibility
 * and testing purposes.
 * <p/>
 * The cache is specifically designed to be self-pruning by removing events older 
 * than the double-click window from the cache when a newer event is added. In order
 * to be self-pruning, it depends on new etnries to be added in chronological order, 
 * but does not enforce it.  The method isLaterOrSameAsLatestCachedTime is provided
 * for users wishing to take action on out-of-order even entry.
 * <p/>
 * Cache pruning is improved over the naive implementation by maintenance of a 
 * housekeeping map structure organized by DateTime to eliminate the need to search
 * for items to remove, as well as limiting pruning to when later log entries are
 * encountered (although millisecond precision of times might limit the usefulness
 * of this approach)
 * 
 * 
 * @author rnahf
 *
 */
public class ReadEventCounterCache {

    private static Logger logger = Logger.getLogger(ReadEventCounterCache.class.getName());
    
    protected Map<String,DateTime> idMap = new HashMap<>();
    protected Map<DateTime,HashSet<String>> timeMap = new HashMap<>();
    private int timeIntervalSeconds;
    private DateTime latestTimeDate = new DateTime(0);
    
    final public static int STANDARD_COUNTER_INTERVAL_PDF = 30;
    
    /**
     * Creates a ReadEventCounterCache with the standard double-click window
     * of 30 seconds. 
     */
    public ReadEventCounterCache() {
        this.timeIntervalSeconds = STANDARD_COUNTER_INTERVAL_PDF;
    }
    
    /**
     * Creates a ReadEventCounterCache with the specified double-click / repeatVisit
     * time window.
     * 
     * @param timeWindowSeconds
     */
    public ReadEventCounterCache(int timeWindowSeconds) {
        this.timeIntervalSeconds = timeWindowSeconds;
    }

    /**
     * gets the number of IDs in the Cache
     * @return
     */
    public int size() {
        return idMap.size();
    }
    
    /**
     * A method to determine if an eventKey is present in the cache.
     * @param eventKey
     * @return
     */
    public boolean contains(String eventKey) {
        return idMap.containsKey(eventKey);
    }
    
    /**
     * Useful for checking that log Entries are being processed in chronological order.
     * @param testValue
     * @return
     */
    public boolean isLaterOrSameAsLatestCachedTime(DateTime testValue) {
        return !testValue.isBefore(this.latestTimeDate);
    }
    
    /**
     * Used to determine if an event is a repeatVisit.  Does not change the cache.
     * 
     * @param eventKey
     * @param value
     * @return
     */
    public boolean isRepeatVisit(String eventKey, DateTime value) {
        if (idMap.containsKey(eventKey))
          if (idMap.get(eventKey).plusSeconds(timeIntervalSeconds).isAfter(value))
            return true;

        return false;
    }
    
    /**
     * Puts a read event into the cache, with the same add / replace semantics as Map.put.
     * Updates housekeeping structures and triggers cache cleanup if the time value
     * is later than exists in the cache.
     * 
     * @param eventKey
     * @param value
     */
    public void putEvent(String eventKey, DateTime value) {
        
        if (value == null) 
            throw new NullPointerException("Cannot have null DateTime value!");
        
        // remove eventKey from the housekeeping timeMap, if there
        if (idMap.containsKey(eventKey)) {
            DateTime oldDT = idMap.get(eventKey);
            HashSet<String> entries = timeMap.get(oldDT);
            entries.remove(eventKey);
  //          logger.info("removing stale event record from timeMap " + eventKey);
        }
        // add or replace the eventKey value
        idMap.put(eventKey, value);
  //      logger.info("added / replaced idMap entry for  " + eventKey);
        
        
        // add item to the housekeeping timeMap 
        if (timeMap.containsKey(value)) {
            timeMap.get(value).add(eventKey);
        } 
        else {
            timeMap.put(value, new HashSet<String>());
            timeMap.get(value).add(eventKey);
        }
        
        if (value.isAfter(latestTimeDate)) {
            latestTimeDate = value;

            // a new time value is a good 
            // indication that we need to
            // purge out-of-window entries
            pruneOutdatedCacheEntries();
        }
    }
    
    
    /**
     * Removes items from the cache that are more than the configured seconds
     * older than the latest time in the cache.
     * 
     * @return the number of items pruned
     */
    public int pruneOutdatedCacheEntries() {
        
        int itemsPruned = 0;
        DateTime purgeCutoff = latestTimeDate.minus(this.timeIntervalSeconds * 1000);
   //     logger.info("purging readEventCache entries from " + purgeCutoff.toString());
        
        DateTime oldestTimepoint = new DateTime();
        
        Iterator<DateTime> it = timeMap.keySet().iterator();
        while (it.hasNext()) {
            DateTime timepoint = it.next();
            
            // only for sanity-checking 
            if (timepoint.isBefore(oldestTimepoint))
                oldestTimepoint = timepoint;
            
            if (timepoint.isBefore(purgeCutoff)) {
    //            logger.info("purging timepoint " + timepoint.toString());
                // remove all entries with outdated latest-times
                for (String eventKey : timeMap.get(timepoint)) {
                    idMap.remove(eventKey);
   //                 logger.info("removing stale event record from timeMap " + eventKey);
                    itemsPruned++;
                }
                // finally remove this housekeeping element, because it did its job.
                it.remove();
            }
        }
    //    logger.info(String.format("Removed %d items from the counter cache", itemsPruned));
        return itemsPruned;
    }
}
