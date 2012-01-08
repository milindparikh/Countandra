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
