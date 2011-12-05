

import java.nio.ByteBuffer; 
import java.util.ArrayList; 
import java.util.HashMap; 
import java.util.List; 
import java.util.Map; 


import org.apache.cassandra.thrift.CassandraServer; 
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



import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import org.slf4j.Logger;import org.slf4j.LoggerFactory;
import me.prettyprint.hector.api.beans.Row;import me.prettyprint.hector.api.beans.Rows;


public class CassandraStorage 
{ 
    private static final int MAX_COLUMNS = 1000; 
    static public CassandraServer server = new CassandraServer(); 

    static Keyspace ksp;
    static Cluster myCluster;
    
    static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();
    static DynamicCompositeSerializer dcs = new DynamicCompositeSerializer(); 
    static ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();    

    private static Logger log = LoggerFactory.getLogger(CassandraStorage.class); 

    static {
	Cluster myCluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");
	ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
	ksp = HFactory.createKeyspace("COUNTANDRA", myCluster, ccl);
    }
    

    public CassandraStorage(){ 

    } 

    /* 
     * Set's thread local value on the server. 
     */ 
    public void setKeyspace(String keyspace) throws Exception 
    { 
	server.set_keyspace(keyspace); 
    } 

    public void addKeyspace(String keyspace) throws Exception 
    { 

	List<CfDef> cfDefList = new ArrayList<CfDef>(); 
	KsDef ksDef = new KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", cfDefList); 
	ksDef.putToStrategy_options("replication_factor", "1"); 
	server.system_add_keyspace(ksDef); 
    } 

    public void dropColumnFamily(String columnFamily) throws Exception 
    { 
	server.system_drop_column_family(columnFamily); 
    } 

    public void dropKeyspace(String keyspace) throws Exception 
    { 
	server.system_drop_keyspace(keyspace); 
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
	server.system_add_column_family(columnFamily); 
    } 



    public void incrementCounter(String rowKey, String columnKey, String timeDimension, long time, int value) {

	try {
	    Mutator<String> m= HFactory.createMutator(ksp, stringSerializer);
	    DynamicComposite dcolKey = new DynamicComposite(); 
	    dcolKey.addComponent(timeDimension, StringSerializer.get()); 
	    dcolKey.addComponent(time, LongSerializer.get()); 


	    m.addCounter(rowKey+":"+columnKey, "DDCC",        
			 HFactory.createCounterColumn(dcolKey, (long) value,
						      new DynamicCompositeSerializer()));
	    


	    m.execute();
	}
	catch (Exception e) {
	    System.out.println(e);

	}

    }


    

    public void setColumn(String keyspace, String column_family, String key, JSONObject json, 
			  ConsistencyLevel consistency_level) throws Exception 
    { 
	this.setColumn(keyspace, column_family, key, json, consistency_level, System.currentTimeMillis()); 
    } 

    public void setColumn(String keyspace, String column_family, String key, JSONObject json, 
			  ConsistencyLevel consistency_level,  long timestamp) throws Exception 
    { 
	List<Mutation> slice = new ArrayList<Mutation>(); 
	for (Object field : json.keySet()) 
	    { 
		String name = (String) field; 
		String value = (String) json.get(name); 
		Column c = new Column(); 
		c.setName(ByteBufferUtil.bytes(name)); 
		c.setValue(ByteBufferUtil.bytes(value)); 
		c.setTimestamp(timestamp); 

		Mutation m = new Mutation(); 
		ColumnOrSuperColumn cc = new ColumnOrSuperColumn(); 
		cc.setColumn(c); 
		m.setColumn_or_supercolumn(cc); 
		slice.add(m); 
	    } 
	Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>(); 
	Map<String, List<Mutation>> cfMutations = new HashMap<String, List<Mutation>>(); 
	cfMutations.put(column_family, slice); 
	mutationMap.put(ByteBufferUtil.bytes(key), cfMutations); 
	server.batch_mutate(mutationMap, consistency_level); 

    } 



}