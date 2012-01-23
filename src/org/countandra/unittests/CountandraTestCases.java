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
import org.junit.Test;

public class CountandraTestCases {
	public static void main(String[] args) {
		org.junit.runner.JUnitCore.main(CountandraTestCases.class.getName());
	}

	@Test(timeout = 45000)
	public void testData() throws Throwable {

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

		// Testing for Sums
		new Thread(new Runnable() {
			public void run() {

				assertEquals(
						"Data Value wrong for category with depth of 3 (sums) for last hour",
						(Long) 500L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY"));
			}
		}).start();
		new Thread(new Runnable() {
			public void run() {

				assertEquals(
						"Data Value wrong for category with depth of 3 (sums) for last hour",
						(Long) 250L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia.augusta/LASTHOUR/MINUTELY"));
			}
		}).start();
		new Thread(new Runnable() {
			public void run() {

				assertEquals(
						"Data Value wrong for category with depth of 2 (sums) for last hour",
						(Long) 750L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY"));
			}
		}).start();
		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 1 (sums) for last hour",
						(Long) 1000L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY"));

			}
		}).start();

		// Testing for Counts
		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 3 (counts) for last hour",
						(Long) 2L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY/COUNTS"));

			}
		}).start();
		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 2 (counts) for last hour",
						(Long) 3L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY/COUNTS"));

			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 1 (counts) for last hour",
						(Long) 4L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY/COUNTS"));
			}
		}).start();

		// Testing for Squares
		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 3 (squares) for last hour",
						(Long) 130000L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia.atlanta/LASTHOUR/MINUTELY/SQUARES"));

			}
		}).start();
		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 2 (squares) for last hour",
						(Long) 192500L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us.georgia/LASTHOUR/MINUTELY/SQUARES"));

			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				assertEquals(
						"Data Value wrong for category with depth of 1 (squares) for last hour",
						(Long) 255000L,
						(Long) CountandraTestUtils
								.httpGet("http://localhost:8080/query/pti/us/LASTHOUR/MINUTELY/SQUARES"));
			}
		}).start();
		CountandraTestUtils.close();

	}

}
