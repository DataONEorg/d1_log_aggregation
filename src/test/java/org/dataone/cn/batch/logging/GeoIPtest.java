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

import com.maxmind.geoip.*;

import org.dataone.cn.batch.logging.GeoIPService;
import org.dataone.configuration.Settings;

import java.net.UnknownHostException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* Test the GeoPI Java API that is used to convert an IP address to location name */
public class GeoIPtest extends TestCase {

	private GeoIPService geoIPsvc;

	/**
	 * Constructor to build the test
	 * 
	 * @param name
	 *            the name of the test method
	 */
	public GeoIPtest(String name) {
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
		suite.addTest(new GeoIPtest("testCity"));

		return suite;
	}

	/**
	 * Use GeoIPservice to convert an IP address to a city name. This tests that
	 * the service has been properly initialized and can read the specified IP
	 * address from the database.
	 * 
	 * @throws Exception
	 */
	public void testCity() throws Exception {

		// For testing, look for this file locally.
        String dbFilename = Settings.getConfiguration().getString(
				"LogAggregator.geoIPdbName");
		//geoIPsvc = GeoIPService.getInstance("./src/test/resources/org/dataone/cn/batch/logging/GeoLiteCity.dat");
		geoIPsvc = GeoIPService.getInstance(dbFilename);

		// IP address for NCEAS
		GeoIPService.GeoIpLocation location = geoIPsvc.getLocation("128.111.84.40");
		String cityName = location.getCity();
		assert (cityName.equals("Santa Barbara"));
		double latitude = location.getLatitude();
		double longitude = location.getLongitude();
		float delta = 0.0002f;
		assertEquals(latitude, 34.4329f, delta);
		assertEquals(longitude, -119.8370f, delta);

		geoIPsvc.close();
	}
}