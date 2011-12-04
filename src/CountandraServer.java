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


public class CountandraServer {
    
    public static Options options = new Options(); 
    static {
	options.addOption("s", "server-mode", false, " Cassandra Server Mode");
	options.addOption("i", "init", false, " Initialize Cassandra with basic structures");
	options.addOption("h", "hector", false, " Test Hector");

    }

    
   
    public static void main (String args[])  {

	try {
	    System.out.println(args[0]);

	    CommandLineParser parser = new PosixParser();
	    CommandLine line = parser.parse( options, args );
	    
	    if (line.hasOption("s")) {
		CountandraUtils.startupCassandraServer();
		if (line.hasOption("i")) {
		    CountandraUtils.initBasicDataStructures();
		}
	    }
	    if (line.hasOption("h")) {
		CountandraUtils.populateTestData();
			CountandraUtils.printResults();
	    }

	    
	    
	    /*
	    List<CfDef> cfDefList = new ArrayList<CfDef>();         
	    KsDef ksDef = new KsDef("abc", "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
	    ksDef.putToStrategy_options("replication_factor", "1");         


	    KsDef ksDef2 = new KsDef("xyz", "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
	    ksDef2.putToStrategy_options("replication_factor", "1");  




	    server.system_add_keyspace(ksDef); 
	    server.system_add_keyspace(ksDef2); 

	    */

	
	    /*
	    List <KsDef> lkd= server.describe_keyspaces();
	    System.out.println(lkd.size());

	    for (int i=0; i< lkd.size(); i++) {
		System.out.println("++++++++++++++++++++++");

		System.out.println(lkd.get(i));
		System.out.println("++++++++++++++++++++++");
	    }
	

	    server.system_drop_keyspace("abc"); 

	    */
	    /*
	    server.set_keyspace("xyz");
	    
	    CfDef columnFamily = new CfDef("xyz", "cf1");
	    columnFamily.setKey_validation_class("UTF8Type");
	    columnFamily.setComparator_type("UTF8Type");
	    columnFamily.setDefault_validation_class("UTF8Type");
	    server.system_add_column_family(columnFamily); 
	    */


	    //	NettyUtils.startupNettyServer(); 
	
	} 
	catch (IOException ioe) {
	    System.out.println(ioe);
	}
	catch (Exception e) {
	    System.out.println(e);
	}
    }
}
