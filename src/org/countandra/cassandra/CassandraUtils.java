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

package org.countandra.cassandra;

import java.io.*;
import org.apache.cassandra.thrift.CassandraDaemon;

public class CassandraUtils {
	private static boolean cassandraStarted = false;
	private static CassandraDaemon daemon = null;
	private static String CASSANDRA_CONFIG_URL = "cassandra.yaml";

	public static synchronized void startupCassandraServer() throws IOException {
		if (cassandraStarted)
			return;

		cassandraStarted = true;

		System.setProperty("cassandra-foreground", "1");
		System.setProperty("cassandra.config", CASSANDRA_CONFIG_URL);
		try {
			// run in own thread
			new Thread(new Runnable() {

				public void run() {
					daemon = new CassandraDaemon();
					daemon.activate();
				}
			}).start();
		} catch (Throwable e) {

			e.printStackTrace();
			System.exit(2);
		}
	}

}
