import java.io.*;
import org.apache.cassandra.thrift.CassandraDaemon;

public class CassandraUtils {
    private static boolean               cassandraStarted       = false;
    private static CassandraDaemon       daemon                 = null;
    private static String CASSANDRA_CONFIG_URL = "cassandra.yaml";

    public static synchronized void startupCassandraServer() throws IOException {
	if (cassandraStarted)
	    return;

        cassandraStarted = true;

	System.setProperty("cassandra-foreground", "1");	
	System.setProperty("cassandra.config", CASSANDRA_CONFIG_URL);              

	 
	try
	    {
		// run in own thread
		new Thread(new Runnable() {
			 
			public void run()
			{
			    daemon = new CassandraDaemon();
			    daemon.activate();
			}
		    }).start();
	    }
        catch (Throwable e)
	    {

		e.printStackTrace();
		System.exit(2);
	    }
    }

    public static synchronized void populateTestData() {
	CountandraUtils cu = new CountandraUtils();
	long currentTime = System.currentTimeMillis();
	
	cu.increment("pti", "com.amazon.music", currentTime, 100);
	cu.increment("pti", "com.amazon.video", currentTime, 200);

	
	cu.increment("pti", "com.amazon.video", currentTime - 1000*3600, 200);  // 1 hour ago
	cu.increment("pti", "com.amazon.music", currentTime - 1000*3600, 200);  // 1 hour ago
	cu.increment("pti", "com.amazon.books", currentTime - 1000*3600, 200);  // 1 hour ago

	
	cu.increment("pti", "com.amazon.video", currentTime - 1000*3600*24, 300);  // 1 day ago
	cu.increment("pti", "com.amazon.music", currentTime - 1000*3600*24, 100);  // 1 day ago
	cu.increment("pti", "com.amazon.books", currentTime - 1000*3600*24, 100);  // 1 day ago

	cu.increment("pti", "com.amazon.books", currentTime - 1000L*3600L*24L*31L, 78);  // 1 month ago
	/*
	cu.increment("pti", "com.amazon.music", currentTime - 1000*3600*24*31, 100);  // 1 month ago


	cu.increment("pti", "com.amazon.books", currentTime - 1000*3600*24*31*112, 100);  // m years ago
	cu.increment("pti", "com.amazon.video", currentTime - 1000*3600*24*31*112, 100);  // m years ago
	*/
	System.out.println(currentTime);
	

    }
    
    public static synchronized void initBasicDataStructures() throws IOException, Exception {
	CassandraStorage cs = new CassandraStorage();
	cs.addKeyspace("COUNTANDRA");
	cs.setKeyspace("COUNTANDRA");

	cs.createColumnFamily("COUNTANDRA", "MetaData");

	/* KeyValues */

	/*
	  KeyValue: KV
	  CompositeKeyValue: CKV
	  DynamicCompositeKeyValue: DCKV
	*/

	
	cs.createColumnFamily("COUNTANDRA", "KV");
	cs.createColumnFamily("COUNTANDRA", "CKV", "UTF8Type","CompositeType(UTF8Type, UTF8Type)","UTF8Type" );
	cs.createColumnFamily("COUNTANDRA", "DCKV", "UTF8Type","DynamicCompositeType (a=>AsciiType,b=>BytesType,i=>IntegerType,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType,A=>AsciiType(reversed=true),B=>BytesType(reversed=true),I=>IntegerType(reversed=true),X=>LexicalUUIDType(reversed=true),L=>LongType(reversed=true),T=>TimeUUIDType(reversed=true),S=>UTF8Type(reversed=true),U=>UUIDType(reversed=true))","UTF8Type" );	
	cs.createColumnFamily("COUNTANDRA", "DCKVI", "UTF8Type","DynamicCompositeType(p=>IntegerType )","UTF8Type" );	

	/* Disributed Counters */

	/*
	  DistributedCounters:DC
	  DistributedCompositeCounters: DCC
	  DistributedDynamicCompositeCounters: DDCC
	*/

	cs.createColumnFamily("COUNTANDRA", "DC", "UTF8Type", "UTF8Type","CounterColumnType" );
	cs.createColumnFamily("COUNTANDRA", "DCC", "UTF8Type","CompositeType(UTF8Type,UTF8Type)","CounterColumnType" );
	cs.createColumnFamily("COUNTANDRA", "DDCC", "UTF8Type", "DynamicCompositeType (a=>AsciiType,b=>BytesType,i=>IntegerType,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType,A=>AsciiType(reversed=true),B=>BytesType(reversed=true),I=>IntegerType(reversed=true),X=>LexicalUUIDType(reversed=true),L=>LongType(reversed=true),T=>TimeUUIDType(reversed=true),S=>UTF8Type(reversed=true),U=>UUIDType(reversed=true))" ,"CounterColumnType" );



    }
    


}
