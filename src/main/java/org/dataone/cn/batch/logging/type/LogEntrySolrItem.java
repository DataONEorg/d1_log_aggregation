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
 * $Id$
 */

package org.dataone.cn.batch.logging.type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.beans.Field;
import org.dataone.client.ObjectFormatCache;
import org.dataone.cn.batch.logging.GeoIPService;
import org.dataone.cn.batch.logging.LogAccessRestriction;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.joda.time.DateTime;
import org.joda.time.Period;

import ch.hsr.geohash.GeoHash;

import java.util.Date;
import java.util.List;
/**
 *  Allows the LogEntry domain object to be mapped to a Solr POJO
 * 
 * @author waltz
 */
public class LogEntrySolrItem implements Serializable {
	
    private static Logger logger = Logger.getLogger(LogEntrySolrItem.class.getName());
    
    private final static String PASSED_ROBOT_TESTS_NONE = "none";
    private final static String PASSED_ROBOT_TESTS_ALL = "all";
    private final static String PASSED_ROBOT_TEST_LOOSE_ONLY = "looseOnly";
    private final static String PASSED_ROBOT_TEST_STRICT_ONLY = "strictOnly";
    
    @Field("id")
    String id;

    @Field("dateAggregated")
    Date dateAggregated;

    @Field("entryId")
    String entryId;

    // Currently (20140612) this field appears to be unused
    @Field("isPublic")
    boolean isPublic = false;

    // really the list of subjects that have read Permission
    @Field("readPermission")
    List<String> readPermission;

    @Field("pid")
    String pid;

    @Field("ipAddress")
    String ipAddress;

    @Field("userAgent")
    String userAgent;

    @Field("subject")
    String subject;

    @Field("event")
    String event;

    @Field("dateLogged")
    Date dateLogged;
    
    @Field("dateUpdated")
    Date dateUpdated;

    @Field("nodeId")
    String nodeIdentifier;
    
    @Field("formatId")
    String formatId;
    
    @Field("formatType")
    String formatType;
    
    @Field("size")
    long size;
    
    @Field("rightsHolder")
    String rightsHolder;
    
    @Field("country")
    String country;
    
    @Field("region")
    String region;
    
    @Field("city")
    String city;
    
    @Field("geohash_1")
    String geohash_1;
    
    @Field("geohash_2")
    String geohash_2;
    
    @Field("geohash_3")
    String geohash_3;
    
    @Field("geohash_4")
    String geohash_4;
    
    @Field("geohash_5")
    String geohash_5;
    
    @Field("geohash_6")
    String geohash_6;
    
    @Field("geohash_7")
    String geohash_7;
    
    @Field("geohash_8")
    String geohash_8;
    
    @Field("geohash_9")
    String geohash_9;
    
    @Field("location")
    String location;

    @Field("repeatVisit")
    Boolean repeatVisit;
    
    @Field("robotChecks")
    String robotChecks;

    public LogEntrySolrItem() {

    }
    
    public LogEntrySolrItem(LogEntry item) {
        this.entryId = item.getEntryId();
        this.pid = item.getIdentifier().getValue();
        this.ipAddress = item.getIpAddress();
        this.userAgent = item.getUserAgent();
        this.subject = item.getSubject().getValue();
        this.event = item.getEvent().xmlValue();
        this.dateLogged = item.getDateLogged();
        this.nodeIdentifier = item.getNodeIdentifier().getValue();
		this.setRobotChecks(PASSED_ROBOT_TESTS_NONE);
		this.setRepeatVisit(false);
    }

	/*
	 * Fill in the solrItem fields for fields that are either obtained
	 * from systemMetadata (i.e. formatId, size for a given pid)
	 * 
	 * @param systemMetadata system metadata object associated with the pid for this log entry
	 */
    
	public void updateSysmetaFields(SystemMetadata systemMetadata) {

		boolean isPublicSubject = false;
	    LogAccessRestriction logAccessRestriction = new LogAccessRestriction();
	    String formmatId = null;
		/* Populate the fields that come from systemMetadata.
		 */
		if (systemMetadata != null) {
			formatId = systemMetadata.getFormatId().getValue();
			this.setFormatId(formatId);
			if (formatId != null) {
				ObjectFormat format = null;
				try {
					ObjectFormatIdentifier objectFormat = new ObjectFormatIdentifier();
					objectFormat.setValue(formatId);
					format = ObjectFormatCache.getInstance().getFormat(
							objectFormat);
					this.setFormatType(format.getFormatType());
				} catch (BaseException e) {
					logger.warn("Unable to obtain formatType for pid "
							+ this.getPid() + ": " + e.getMessage());
				}
			}
			
			List<String> subjectsAllowedRead = logAccessRestriction.subjectsAllowedRead(systemMetadata);
			this.setIsPublic(isPublicSubject);
			this.setReadPermission(subjectsAllowedRead);
			this.setSize(systemMetadata.getSize().longValue());
			this.setRightsHolder(systemMetadata.getRightsHolder().getValue());
			this.setIsPublic(isPublicSubject);
		}
	}
	
	/*
	 * Fill in the solrItem fields for fields that are
	 * derived from the ipAddress from the logEntry (i.e. city, state,
	 * geohash_* are derived from the ipAddress in the logEntry)
	 *
	 * @param geoIPsvc GeoIP service instance
	 */
	public void updateLocationFields(GeoIPService geoIPsvc) {

		String geohash = null;
		double geohashLat = 0;
		double geohashLong = 0;
		
		// Geohashes will be stored at different lengths which can either be used for determining pid counts for regions of a map
		// at different resolutions, or for searching/filtering
		// Length of geohash to retrieve from service
		int geohashLength = 9;

		// Set the geographic location attributes determined from the IP address
		// This will be stored in the Solr index as a geohash and as lat, long
		// spatial type
		if (geoIPsvc != null && this.getIpAddress() != null) {
			// Set the geographic location attributes determined from the IP
			// address
			geoIPsvc.initLocation(this.getIpAddress());
			// Add the location attributes to the current Solr document
			this.setCountry(geoIPsvc.getCountry());
			this.setRegion(geoIPsvc.getRegion());
			this.setCity(geoIPsvc.getCity());
			// Calculate the geohash values based on the lat, long returned from
			// the GeoIP service.
			geohashLat = geoIPsvc.getLatitude();
			geohashLong = geoIPsvc.getLongitude();
			String location = String.format("%.4f", geohashLat) + ", "
					+ String.format("%.4f", geohashLong);
			System.out.println("location: " + location);
			this.setLocation(location);
			try {
				geohash = GeoHash.withCharacterPrecision(geohashLat,
						geohashLong, geohashLength).toBase32();
				this.setGeohash_1(geohash.substring(0, 1));
				this.setGeohash_2(geohash.substring(0, 2));
				this.setGeohash_3(geohash.substring(0, 3));
				this.setGeohash_4(geohash.substring(0, 4));
				this.setGeohash_5(geohash.substring(0, 5));
				this.setGeohash_6(geohash.substring(0, 6));
				this.setGeohash_7(geohash.substring(0, 7));
				this.setGeohash_8(geohash.substring(0, 8));
				this.setGeohash_9(geohash.substring(0, 9));
			} catch (IllegalArgumentException iae) {
				logger.error("Error calculating geohash for log record id "
						+ this.getId() + ": " + iae.getMessage());
			}
		}
	}
    
	/*
	 * Fill in the fields related to COUNTER compliance
	 *
	 * @param robotsLoose
	 * @param robotsStrict
	 * @param readEventCache
	 * @param eventsToCheck
	 * @param repeatVisitIntervalSeconds
	 */
	public void setCOUNTERfields(ArrayList<String> robotsLoose, ArrayList<String> robotsStrict,
			HashMap<String, DateTime> readEventCache, HashSet<String> eventsToCheck, int repeatVisitIntervalSeconds) {

		String IPaddress = this.getIpAddress();
		DateTime readEventTime = new DateTime(this.getDateLogged());
	    DateTime intervalEndTime;
	    Period repeatVisitPeriod = new Period().withSeconds(repeatVisitIntervalSeconds);
	    Pattern robotPattern;
	    Matcher robotPatternMatcher;
	    boolean robotMatches;
		
		// Check if the event for this record is one that we are checking for COUNTER.
		// If not, then return with default values.
		if (! eventsToCheck.contains(this.event.trim().toLowerCase())) {
			return;
		}
		
		Boolean passedRobotTestStrict = true;
		Boolean passedRobotTestLoose = true;
		
		// Iterate over less restrictive list of robots, comparing as regex to the user-agent of
		// the current record.
		for (String robotRegex : robotsLoose) {
			robotPattern = Pattern.compile(robotRegex.trim());
			robotPatternMatcher = robotPattern.matcher(this.userAgent.trim());
	        robotMatches = robotPatternMatcher.matches();
	        if (robotMatches) {
	        	passedRobotTestLoose = false;
	        	break;
	        }
		}
		
		// Iterate over strict list of robots, comparing as regex to the user-agent of
		// the current record.
		for (String robotRegex : robotsStrict) {
			robotPattern = Pattern.compile(robotRegex.trim());
			robotPatternMatcher = robotPattern.matcher(this.userAgent.trim());
	        robotMatches = robotPatternMatcher.matches();
	        if (robotMatches) {
	        	passedRobotTestStrict = false;
	        	break;
	        }
		}
		
		// Set the 'robotChecks' based on the tests that have passed. The following chart shows
		// sample user-agents, the corresponding test results and the resulting index field value.
		//
		// user-agent     passes 'loose' test     passes 'strict' test     resulting 'robotChecks' field
		// ----------     --------------------    ---------------------    ----------------------------
        // Mozilla        y                       y                        all
		// google         n                       n                        none
		// java           y                       n                        looseOnly
		// ???            n                       y                        strictOnly
		
		if (passedRobotTestLoose && passedRobotTestStrict) {
		    this.setRobotChecks(PASSED_ROBOT_TESTS_ALL);
		} else if(!passedRobotTestLoose && !passedRobotTestStrict) {
		    this.setRobotChecks(PASSED_ROBOT_TESTS_NONE);
		} else if (passedRobotTestLoose && !passedRobotTestStrict) {
		    this.setRobotChecks(PASSED_ROBOT_TEST_LOOSE_ONLY);
		} else if (!passedRobotTestLoose && passedRobotTestStrict) {
		    this.setRobotChecks(PASSED_ROBOT_TEST_STRICT_ONLY);
		}
			
		DateTime cachedEventTime;
		// Check if this log record is a 'repeat visit', i.e. this is a read
		// event from the same IP address as an earlier request happening
		// within a specified time interval.
		if (readEventCache.containsKey(IPaddress)) {
			// A read event for this IP address was previously cached, so see if the current read event
			// was within 30 seconds of the cached one.
			cachedEventTime = readEventCache.get(IPaddress);
			intervalEndTime = cachedEventTime.plus(repeatVisitPeriod);
			if (readEventTime.isBefore(intervalEndTime) || readEventTime.isEqual(intervalEndTime)) {
				this.setRepeatVisit(true);
			} else {
				// This event entry must be after the repeatEventInterval, so
				// make it the new beginning of the repeatEventInterval for this IP address.
				readEventCache.put(IPaddress, readEventTime);
				this.setRepeatVisit(false);
			}
		} else {
			// No entry for this IP, so create a new one
			readEventCache.put(IPaddress, readEventTime);
			this.setRepeatVisit(false);
		}
		
		return;
	}
    
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
	
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
    
    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

	public String getGeohash_1() {
		return geohash_1;
	}

	public void setGeohash_1(String geohash) {
		this.geohash_1 = geohash;
	}

	public String getGeohash_2() {
		return geohash_2;
	}

	public void setGeohash_2(String geohash) {
		this.geohash_2 = geohash;
	}

	public String getGeohash_3() {
		return geohash_3;
	}

	public void setGeohash_3(String geohash) {
		this.geohash_3 = geohash;
	}

	public String getGeohash_4() {
		return geohash_4;
	}

	public void setGeohash_4(String geohash) {
		this.geohash_4 = geohash;
	}

	public String getGeohash_5() {
		return geohash_5;
	}

	public void setGeohash_5(String geohash) {
		this.geohash_5 = geohash;
	}

	public String getGeohash_6() {
		return geohash_6;
	}

	public void setGeohash_6(String geohash) {
		this.geohash_6 = geohash;
	}

	public String getGeohash_7() {
		return geohash_7;
	}

	public void setGeohash_7(String geohash) {
		this.geohash_7 = geohash;
	}

	public String getGeohash_8() {
		return geohash_8;
	}

	public void setGeohash_8(String geohash) {
		this.geohash_8 = geohash;
	}

	public String getGeohash_9() {
		return geohash_9;
	}

	public void setGeohash_9(String geohash) {
		this.geohash_9 = geohash;
	}
	
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}

    public void setDateAggregated(Date dateAggregated) {
        this.dateAggregated = dateAggregated;
    }

    public Date getDateAggregated() {
        return dateAggregated;
    }

    public Date getDateLogged() {
        return dateLogged;
    }

    public void setDateLogged(Date dateLogged) {
        this.dateLogged = dateLogged;
    }

    public Date getDateUpdated() {
        return dateUpdated;
    }

    public void setDateUpdated(Date dateUpdated) {
        this.dateUpdated = dateUpdated;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getFormatId() {
        return formatId;
    }

    public void setFormatId(String formatId) {
        this.formatId = formatId;
    }   
    
    public String getFormatType() {
        return formatType;
    }

    public void setFormatType(String formatType) {
        this.formatType = formatType;
    } 
    
    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
    
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
    
    public Boolean getRepeatVisit() {
        return repeatVisit;
    }

    public void setRepeatVisit(Boolean repeatVisit) {
        this.repeatVisit = repeatVisit;
    }
    
    public String getRightsHolder() {
        return rightsHolder;
    }

    public void setRightsHolder(String rightsHolder) {
        this.rightsHolder = rightsHolder;
    }
    
    public String getRobotChecks() {
    	return this.robotChecks;
    }

    public void setRobotChecks(String checks) {
        this.robotChecks = checks;
    }
    
    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
    
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public boolean getIsPublic() {
        return isPublic;
    }

    public void setIsPublic(boolean isPublic) {
        this.isPublic = isPublic;
    }

    public List<String> getReadPermission() {
        return readPermission;
    }

    public void setReadPermission(List<String> readPermission) {
        this.readPermission = readPermission;
    }
}
