package org.countandra.cassandra;

import java.nio.ByteBuffer; 
import java.util.ArrayList; 
import java.util.HashMap; 
import java.util.List; 
import java.util.Map; 


import org.apache.cassandra.thrift.CassandraServer; 
import org.apache.cassandra.thrift.Cassandra.*;
import org.apache.cassandra.thrift.Cassandra;

import org.apache.cassandra.thrift.CfDef; 
import org.apache.cassandra.thrift.Column; 
import org.apache.cassandra.thrift.ColumnOrSuperColumn; 
import org.apache.cassandra.thrift.ColumnParent; 
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.ColumnPath; 
import org.apache.cassandra.thrift.ConsistencyLevel; 
import org.apache.cassandra.thrift.KsDef; 
import org.apache.cassandra.thrift.Mutation; 
import org.apache.cassandra.thrift.SlicePredicate; 
import org.apache.cassandra.thrift.SliceRange; 
import org.apache.cassandra.utils.ByteBufferUtil; 
import org.json.simple.JSONObject; 
import org.json.simple.JSONValue; 
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;



import org.slf4j.Logger;import org.slf4j.LoggerFactory;



public class CassandraDB
{ 
    static public CassandraServer server = new CassandraServer(); 
    private static Logger log = LoggerFactory.getLogger(CassandraDB.class); 
    /* 
     * Set's thread local value on the server. 
     */ 

    static private Cassandra.Client client = null;
    static TTransport tr = null;
    static TProtocol proto = null;


    static String cassandraHost = new String("localhost");
    static int cassandraPort = 9160;
     
    static {
	/*	
	tr = new TFramedTransport(new TSocket(cassandraHost, cassandraPort));
	proto = new TBinaryProtocol(tr);
	client = new Cassandra.Client(proto);
	*/
	
    }
    
    
    

    private static String s_locatorStrategy = new String ("org.apache.cassandra.locator.SimpleStrategy");
    private static String s_replicationFactor = new String("1");
    
    public static synchronized void setGlobalParams(String hostIp) {
	String [] split_hostIp = hostIp.split(":");
	cassandraHost = split_hostIp[0];
	cassandraPort = Integer.parseInt(split_hostIp[1]);
    }
    public static synchronized void setGlobalParams(String hostIp, String replicationFactor) {
	setGlobalParams( hostIp);
	s_replicationFactor = replicationFactor;
    }



    public void addKeyspace(String keyspace) throws Exception 
    { 
	addKeyspace(keyspace, s_locatorStrategy, s_replicationFactor);
	
    } 


    public void addKeyspace(String keyspace, String locatorStrategy, String replicationFactor) throws Exception  {
	
	List<CfDef> cfDefList = new ArrayList<CfDef>(); 
	KsDef ksDef = new KsDef(keyspace, locatorStrategy, cfDefList); 
	ksDef.putToStrategy_options("replication_factor", replicationFactor); 

 	tr = new TFramedTransport(new TSocket(cassandraHost, cassandraPort));
	proto = new TBinaryProtocol(tr);
	client = new Cassandra.Client(proto);

	tr.open();
	
	client.system_add_keyspace(ksDef); 

	tr.close();


    }
    
   
    public void createColumnFamily(String keyspace, String columnFamilyName) throws Exception 
    { 
	this.createColumnFamily(keyspace, columnFamilyName, "UTF8Type", "UTF8Type", "UTF8Type");
    } 

    public void createColumnFamily(String keyspace, String columnFamilyName, String keyValidationClass, String comparatorType,String defaultValidationClass) 
	throws Exception 
    { 
	CfDef columnFamily = new CfDef(keyspace, columnFamilyName); 
	columnFamily.setKey_validation_class(keyValidationClass); 
	columnFamily.setComparator_type(comparatorType); 
	columnFamily.setDefault_validation_class(defaultValidationClass); 


	tr = new TFramedTransport(new TSocket(cassandraHost, cassandraPort));
	proto = new TBinaryProtocol(tr);
	client = new Cassandra.Client(proto);

	tr.open();
	client.set_keyspace(keyspace); 
	client.system_add_column_family(columnFamily); 
	tr.close();
	
    } 


}