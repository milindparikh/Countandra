import java.io.*;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.CommandLine;

import org.apache.commons.cli.Options;

import org.countandra.netty.*;
import org.countandra.cassandra.*;
import org.countandra.utils.*;

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
		options.addOption("h", "httpserver", false,
				" Whether to include httserver");
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
			}

			if (line.hasOption("s")) {
				System.out.println("Starting Cassandra");
				// cassandra server
				CassandraUtils.startupCassandraServer();
			}
			if (line.hasOption("i")) {
				if (line.hasOption("cassandrahostip")) {
					System.out.println("Initializing Basic structures");
					CountandraUtils.setCassandraHostIp(line
							.getOptionValue("cassandrahostIp"));
					CountandraUtils.initBasicDataStructures();
					System.out.println("Initialized Basic structures");
				} else {
					System.out.println("Initializing Basic structures");
					CountandraUtils.initBasicDataStructures();
					System.out.println("Initialized Basic structures");
				}
			}

			if (line.hasOption("h")) {

				if (line.hasOption("httpserverport")) {
					httpPort = Integer.parseInt(line
							.getOptionValue("httpserverport"));
				}

				if (line.hasOption("cassandrahostip")) {
					CountandraUtils.setCassandraHostIp(line
							.getOptionValue("cassandrahostIp"));
				} else {
					CountandraUtils.setCassandraHostIp("localhost:9160");
				}

				NettyUtils.startupNettyServer(httpPort);
			}
			// http server

		} catch (IOException ioe) {
			System.out.println(ioe);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
