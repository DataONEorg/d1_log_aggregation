/**
 * This work was created by participants in the DataONE project, and is jointly copyrighted by participating
 * institutions in DataONE. For more information on DataONE, see our web site at http://dataone.org.
 *
 * Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * $Id$
 */

package org.dataone.cn.batch.logging;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

import org.apache.log4j.Logger;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.regionName;
import com.maxmind.geoip.Location;

/**
 * Call the GeoLite (www.maxmind.com) database in order to determine a location
 * from an IP address.
 * 
 * This product includes GeoLite data created by MaxMind, available from <a
 * href="http://www.maxmind.com">http://www.maxmind.com</a>.
 * 
 * @author slaughter
 */
public class GeoIPService {

	private String geoIPdbName = null;
	private LookupService geoIPsvc = null;
	private Logger logger = null;
	private static GeoIPService instance = null;
	private String country = null;
	private String region = null;
	private String city = null;
	private double latitude = 0.0;
	private double longitude = 0.0;

	public String getCountry() {
		return country;
	}

	public String getRegion() {
		return region;
	}

	public String getCity() {
		return city;
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public double getLongitude() {
		return longitude;
	}
	
	/**
	 * Set the location attributes for this object
	 * 
	 * @param IPaddr
	 *            the IP address to obtain location information for
	 */
	
	public void initLocation(String IPaddr) {

		Location location = null;
		country = null;
		region = null;
		city = null;

		// Reopen the database file if we have closed it.
		if (geoIPsvc == null)
			geoIPsvc = getLookupService();

		if (geoIPsvc != null) {
			if (IPaddr != null && geoIPsvc != null) {
				location = geoIPsvc.getLocation(IPaddr);

				if (location != null) {
					country = location.countryName;
					region = regionName.regionNameByCode(
							location.countryCode, location.region);
					city = location.city;
					latitude = location.latitude;
					longitude = location.longitude;
				} else {
					System.out.println("location not found");
				}
				System.out.println("country: " + country + ", region: " + region + ", city: " + city);
			}
		}
	}

	/**
	 * Initialize the GeoIP lookupservice by reading the GeoIP database file.
	 */
	private GeoIPService(String dbFilename) throws FileNotFoundException {

		Logger logger = Logger.getLogger(GeoIPService.class.getName());
		File f = new File(dbFilename);

		// If the specified database file doesn't exist, then use the loca
		if (f.exists()) {
			geoIPdbName = dbFilename;
		} else {
			geoIPdbName = null;
			String msg = "GeoIP database file " + dbFilename
					+ " does not exist";
			logger.error(msg);
			throw new FileNotFoundException(msg);
		}

		try {
			geoIPsvc = getLookupService();
			logger.info("GeoIP service initialized from file " + geoIPdbName);
		} catch (Exception e) {
			logger.error("Error initializing GeoIPService: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @return the single GeoIPService instance
	 */
	public static GeoIPService getInstance(String dbFilename) throws FileNotFoundException {

		if (instance == null) {
			instance = new GeoIPService(dbFilename);
		}

		return instance;
	}

	/**
	 * Create a GeoIP lookup service that will return place names given an IP
	 * address
	 * 
	 * @return LookupService
	 */
	private LookupService getLookupService() {

		LookupService lus = null;

		try {
			lus = new LookupService(geoIPdbName,
					LookupService.GEOIP_MEMORY_CACHE);
		} catch (IOException e) {
			logger.error("Error opening GeoIP database file " + geoIPdbName
					+ ": " + e.getMessage());
		}
		
		return lus;
	}

	/**
	 * Close the GeoIP lookup service. If this singleton is accessed again, the
	 * database will be reopened automatically.
	 */
	public void close() {
		if (geoIPsvc != null) {
			try {
				geoIPsvc.close();
			} catch (Exception e) {
				logger.error("Error closing GeoIP database: " + e.getMessage());
			}
			geoIPsvc = null;
		}
	}
}
