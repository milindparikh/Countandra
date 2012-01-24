/*Copyright 2012  Countandra

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.countandra.unittests;

import static org.junit.Assert.assertEquals;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CountandraTestCases {
	@BeforeClass
	public static void insertTestData() {

		CountandraTestUtils.setPipeLineFactory();
		DateTime dt = new DateTime(DateTimeZone.UTC);

		// Inserting data for last hour

		CountandraTestUtils.insertData("c=pti&s=us.georgia.atlanta&t="
				+ dt.minusHours(1).getMillis() + "&v=200");

		CountandraTestUtils.insertData("c=pti&s=us.georgia.atlanta&t="
				+ dt.minusHours(1).getMillis() + "&v=300");

		CountandraTestUtils.insertData("c=pti&s=us.georgia.augusta&t="
				+ dt.minusHours(1).getMillis() + "&v=250");

		CountandraTestUtils.insertData("c=pti&s=us.california.lasvegas&t="
				+ dt.minusHours(1).getMillis() + "&v=250");

	}

	@AfterClass
	public static void clear() {
		CountandraTestUtils.close();
	}

	@Test
	public void testCase1() {
		try {
			assertEquals(
					"Data Value wrong for category with depth of 3 (sums) for last hour",
					(Long) 500L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY"));

		} catch (AssertionError ae) {
			throwMangledException(ae);
		}
	}

	@Test
	public void testCase2(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 3 (sums) for last hour",
					(Long) 250L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia.augusta/LASTHOUR/MINUTELY"));

		} catch (AssertionError ae) {
			throwMangledException(ae);
		}
	}

	@Test
	public void testCase3(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 2 (sums) for last hour",
					(Long) 750L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY"));

		} catch (AssertionError ae) {
			throwMangledException(ae);
		}
	}

	@Test
	public void testCase4(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 1 (sums) for last hour",
					(Long) 1000L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY"));

		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	// Testing for Counts
	@Test
	public void testCase5(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 3 (counts) for last hour",
					(Long) 2L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY/COUNTS"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	@Test
	public void testCase6(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 2 (counts) for last hour",
					(Long) 3L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY/COUNTS"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	@Test
	public void testCase7(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 2 (counts) for last hour",
					(Long) 3L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY/COUNTS"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	@Test
	public void testCase8(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 1 (counts) for last hour",
					(Long) 4L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY/COUNTS"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}
	}

	// Testing for Squares
	@Test
	public void testCase9(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 3 (squares) for last hour",
					(Long) 130000L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY/SQUARES"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	@Test
	public void testCase10(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 2 (squares) for last hour",
					(Long) 192500L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY/SQUARES"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}

	}

	@Test
	public void testCase11(){
		try {
			assertEquals(
					"Data Value wrong for category with depth of 1 (squares) for last hour",
					(Long) 255000L,
					(Long) CountandraTestUtils
							.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY/SQUARES"));
		} catch (AssertionError ae) {
			throwMangledException(ae);
		}
	}

	private static void throwMangledException(AssertionError ae) {

		StackTraceElement[] stackTrace = ae.getStackTrace();
		StackTraceElement[] newStackTrace = new StackTraceElement[1];
		System.arraycopy(stackTrace, 0, newStackTrace, 0, newStackTrace.length);
		ae.setStackTrace(newStackTrace);
		throw ae;
	}

}
