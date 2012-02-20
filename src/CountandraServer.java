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

import java.io.*;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;

import org.apache.commons.cli.Options;

import org.countandra.netty.*;
import org.countandra.kafka.*;
import org.countandra.cassandra.*;
import org.countandra.unittests.CountandraTestCases;
import org.countandra.unittests.CountandraTestUtils;

import org.countandra.utils.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.runner.JUnitCore;

// CassandraServer is local (with CassandraYaml specify the data store and distributed servers)  
//    -local-cassandra -local-cassandra-thrift-port -local-cassandra-thrift-ip
//      Therefore start the CassandraServer and
//      Initialize the data structures associated with Countandra including some default values
//         on localhost and specified port 9160?
// CassandraServer is remote
//      Assume that the initialization is done seperately and the CassandraServer is started seperately
//      just need a pointer there to the host and port (could be localhost and default port)
//      -remote-cassandra -remote-cassandra-thrift-port -remote-cassandra-thrift-ip

// HttpServer is always assumed to be local and should  be started 
// --http-server -http-server-port

public class CountandraServer {

	public static Options options = new Options();

	static {
		options.addOption("s", "server-mode", false, " Cassandra Server Mode");
		options.addOption("i", "init", false,
				" Initialize Cassandra with basic structures");
		options.addOption("t", "unit-test", false,
				"Test Countandar for sums, counts and squares");
		options.addOption("h", "httpserver", false,
				" Whether to include httserver");

		options.addOption("k", "kafkaconsumer", false,
				" Whether to include kafkaconsumer");

		options.addOption("httpserverport", "httpserverport", true,
				" httpserver port in case the default of 8080 does not work");
		options.addOption("cassandrahostip", "cassandrahostip", true,
				" cassandra host ip for  httpserverto communicate to");
		options.addOption("consistencylevel", "consistencylevel", true,
				" consistency levels of writes/reads");

		options.addOption("replicationfactor", "replicationfactor", true,
				" replicas of data");
		options.addOption("locatorstrategy", "locatorstrategy", true,
				"how the rows should be located");

	}

	private static int httpPort = 8080;
	private static String cassandraServerForClient = new String(
			"localhost:9160");

	public static void main(String args[]) {

		try {
			System.out.println(args[0]);

			CommandLineParser parser = new PosixParser();
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("cassandrahostip")) {
				CountandraUtils.setCassandraHostIp(line
						.getOptionValue("cassandrahostip"));
				if (line.hasOption("consistencylevel")) {
					if (line.hasOption("replicationfactor")) {

						CassandraStorage.setGlobalParams(
								line.getOptionValue("cassandrahostip"),
								line.getOptionValue("consistencylevel"));
						CassandraDB.setGlobalParams(
								line.getOptionValue("cassandrahostip"),
								line.getOptionValue("replicationfactor"));

					} else {

						CassandraStorage.setGlobalParams(
								line.getOptionValue("cassandrahostip"),
								line.getOptionValue("consistencylevel"));
						CassandraDB.setGlobalParams(line
								.getOptionValue("cassandrahostip"));

					}
				}

				else { // no consistency level -- assumed to be ONE
					if (line.hasOption("replicationfactor")) {

						CassandraStorage.setGlobalParams(line
								.getOptionValue("cassandrahostip"));
						CassandraDB.setGlobalParams(
								line.getOptionValue("cassandrahostip"),
								line.getOptionValue("replicationfactor"));

					} else {

						CassandraStorage.setGlobalParams(line
								.getOptionValue("cassandrahostip"));
						CassandraDB.setGlobalParams(line
								.getOptionValue("cassandrahostip"));

					}
				}
			} else {
				CassandraStorage.setGlobalParams(cassandraServerForClient);
				CassandraDB.setGlobalParams(cassandraServerForClient);
			}

			if (line.hasOption("s")) {
				System.out.println("Starting Cassandra");
				// cassandra server
				CassandraUtils.startupCassandraServer();

			}
			if (line.hasOption("i")) {
				System.out.print("Checking if Cassandra is initialized");
				CassandraDB csdb = new CassandraDB();
				while (!csdb.isCassandraUp()) {
					System.out.print(".");
				}
				System.out.println(".");
				System.out.println("Initializing Basic structures");
				CountandraUtils.initBasicDataStructures();
				System.out.println("Initialized Basic structures");

			}

			if (line.hasOption("h")) {

				if (line.hasOption("httpserverport")) {
					httpPort = Integer.parseInt(line
							.getOptionValue("httpserverport"));
				}
				NettyUtils.startupNettyServer(httpPort);
				System.out.println("Started Http Server");
			}


			if (line.hasOption("k")) {

			    KafkaUtils.startupKafkaConsumer();
			    System.out.println("Started Kafka Consumer");
			}

			// Unit Tests
			if (line.hasOption("t")) {
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				org.junit.runner.JUnitCore.main(CountandraTestCases.class
						.getName());
			}

		} catch (IOException ioe) {
			System.out.println(ioe);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
