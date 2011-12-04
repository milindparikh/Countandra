
import java.io.*;
import java.lang.reflect.Field;

import org.apache.cassandra.service.AbstractCassandraDaemon;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.thrift.ConsistencyLevel; 
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
import java.util.regex.*;

import org.apache.log4j.Logger;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.Locale;
import java.util.SimpleTimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.HCounterColumn;

import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.beans.CounterSlice;

public class CountandraUtils {


    static String delimiter = "\\.";
    static String sDelimiter = ".";

    static Cluster myCluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");
    static ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();    
    static {
	ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
    }
    
    static Keyspace ksp  = HFactory.createKeyspace("COUNTANDRA", myCluster, ccl);
    static DynamicCompositeSerializer dcs = new DynamicCompositeSerializer(); 
    static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();    
    static public CassandraServer server = new CassandraServer(); 
    static Pattern isNumber = Pattern.compile("[0-9]");

    
    /* Time Dimensions to store against
       
       m = minutely
       H = hourly
       D = Daily
       M = Monthly
       Y = Yearly
       A = All Time
       
    */
    

	

    public enum TimeDimension {
	MINUTELY(10, "m"), HOURLY(20, "H"), DAILY(30, "D"), MONTHLY(40, "M"), YEARLY(50, "Y"), ALLTIME(100, "A");
	private int code;
	private String sCode;
	
	
	private TimeDimension(int c, String s) {
	    code = c;
	    sCode = s;
	}
	public int getCode() {
	    return code;
	}
	public String getSCode() {
	    return sCode;
	}
    }

    
    static HashMap <String, TimeDimension> hshSupportedTimeDimensions = new HashMap<String, TimeDimension> ();
    static {
	hshSupportedTimeDimensions.put("m", TimeDimension.MINUTELY);
	hshSupportedTimeDimensions.put("H", TimeDimension.HOURLY);
	hshSupportedTimeDimensions.put("D", TimeDimension.DAILY);
	hshSupportedTimeDimensions.put("M", TimeDimension.MONTHLY);
	hshSupportedTimeDimensions.put("Y", TimeDimension.YEARLY);
	hshSupportedTimeDimensions.put("A", TimeDimension.ALLTIME);
    }
    
    

    static String defaultTimeDimensions = "m,H,D,M,Y,A";


    private static final Logger          logger                 = Logger.getLogger(CountandraUtils.class);
    private static boolean               cassandraStarted       = false;
    private static CassandraDaemon       daemon                 = null;
    private static String CASSANDRA_CONFIG_URL = "cassandra.yaml";

    private static boolean nettyStarted = false;
    private static StringBuffer buf = new StringBuffer();    


    public void denormalizedIncrement(String category, String ptimeDimensions, String denormalizedKey, long time, int value) {
	CassandraStorage cs = new CassandraStorage();
	
	DateTime dt = new DateTime(time);
	
	DateTime dtm = new DateTime (dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), dt.getHourOfDay(),dt.getMinuteOfHour());
	DateTime dtH = new DateTime (dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), dt.getHourOfDay(),0);
	DateTime dtD = new DateTime (dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), 0 ,0);
	DateTime dtM = new DateTime (dt.getYear(), dt.getMonthOfYear(), 1, 0, 0);
	DateTime dtY = new DateTime (dt.getYear(), 1, 1, 0, 0);
	String [] timeDimensions = ptimeDimensions.split(",");
	
	
	for (int i = 0; i < timeDimensions.length;i++) {
	    switch (hshSupportedTimeDimensions.get(timeDimensions[i])) {
	    case MINUTELY:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.MINUTELY.getSCode() + ":" + dtm.getMillis());
		cs.incrementCounter(category, denormalizedKey, TimeDimension.MINUTELY.getSCode(), dtm.getMillis(), value);
		
		
		break;
	    case HOURLY:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.HOURLY.getSCode() + ":" + dtH.getMillis());

		cs.incrementCounter(category, denormalizedKey, TimeDimension.HOURLY.getSCode(), dtH.getMillis(), value);
		break;
	    case DAILY:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.DAILY.getSCode() + ":" + dtD.getMillis());

		cs.incrementCounter(category, denormalizedKey, TimeDimension.DAILY.getSCode(), dtD.getMillis(), value);
		break;
	    case MONTHLY:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.MONTHLY.getSCode() + ":" + dtM.getMillis());
		
		cs.incrementCounter(category, denormalizedKey, TimeDimension.MONTHLY.getSCode(), dtM.getMillis(), value);
		break;
	    case YEARLY:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.YEARLY.getSCode() + ":" + dtY.getMillis());
		cs.incrementCounter(category, denormalizedKey, TimeDimension.YEARLY.getSCode(), dtY.getMillis(), value);
		break;
	    case ALLTIME:
		System.out.print(category + ":" + denormalizedKey + ":");
		System.out.println(TimeDimension.ALLTIME.getSCode() + ":" + 0);
		cs.incrementCounter(category, denormalizedKey, TimeDimension.ALLTIME.getSCode(), 0L, value);
		break;

	    }
	}
    }

    // GET WEEKLY COUNTS FOR pti
    public static void printResults() {
	CountandraUtils cu = new CountandraUtils();
	
	ResultStatus result = cu.executeQuery("pti", "com.amazon", "H", "11-03-2011|12-05-2011", "Pacific");
	System.out.println("| Result executed in: {} microseconds against host: {}");
	System.out.println(result.getExecutionTimeMicro());
	System.out.println(result.getHostUsed().getName());

        if ( result instanceof QueryResult ) {
	    System.out.println(((QueryResult) result).get());
            QueryResult<?> qr = (QueryResult)result;

	    if ( qr.get() instanceof CounterSlice ) {
		List <HCounterColumn <DynamicComposite>> lCols = ((CounterSlice)qr.get()).getColumns();
		
		System.out.println(lCols.size());
		for (int i = 0; i < lCols.size(); i++) {
		    me.prettyprint.cassandra.model.HCounterColumnImpl  hcc =  (me.prettyprint.cassandra.model.HCounterColumnImpl ) lCols.get(i);

		    
		    DynamicComposite nameHcc = (DynamicComposite) hcc.getName();
		    System.out.println(hcc.getName());		    
		    System.out.println(hcc.getValue());
		    System.out.println(nameHcc.size());
		} 
	    }
        }
    }


   

    private String getKey(String category, String subTree) {
	return category+":"+subTree;
    }


    public ResultStatus executeQuery(String category, String subTree, String timeDimensions, String timeQuery, String timeZone) {
	// MM-DD-YYYY:hh-mm | MM-DD-YYYY:hh-mm
	long startDateMillis = 0L;
	long endDateMillis = 0L;

	System.out.println("----------------------------------------------");
	
	System.out.println(category);
	System.out.println(subTree);
	
	Matcher matcher = isNumber.matcher(timeQuery);
	if (matcher.find()) {
	    String s[] = timeQuery.split("\\|");

	    String startDate = s[0];
	    String endDate = s[1];
	
	    startDateMillis = getLongMillis(startDate);
	    endDateMillis = getLongMillis(endDate);

	}

	System.out.println(startDateMillis);
	System.out.println(endDateMillis);
	
	System.out.println("----------------------------------------------");
	return executeQuery(category, subTree, timeDimensions, startDateMillis, endDateMillis, timeZone);
	
    }


    private ResultStatus executeQuery( String category, String subtree, String timeDimension, long startTime, long endTime, String timeZone) {
	SliceCounterQuery<String, DynamicComposite> sliceCounterQuery = HFactory.createCounterSliceQuery(ksp, stringSerializer, dcs); 
	sliceCounterQuery.setColumnFamily("DDCC");   
	sliceCounterQuery.setKey(getKey(category,subtree));   
	DynamicComposite startRange = getStartRange( timeDimension, startTime, endTime);
	DynamicComposite endRange = getEndRange( timeDimension, startTime, endTime);
	sliceCounterQuery.setColumnNames(startRange);
	sliceCounterQuery.setRange(startRange,endRange, false, 100);
	QueryResult  result = sliceCounterQuery.execute();    
	return result;
    }

	
    public long getLongMillis(String theDate) {
	
	// MM-DD-YYYY:hh-mm
	
	int hour = 0;
	int min = 0;
	int sec = 0;
	int year;
	int month;
	int day;
	
	
	String currentDate = theDate;
	
	if (theDate.matches(":")) {
	    String splitDates [] = theDate.split(":");
	    currentDate = splitDates[0];
	}
	String splitDates[] = currentDate.split("-");
	month = Integer.parseInt(splitDates[0]);
	day = Integer.parseInt(splitDates[1]);
	year = Integer.parseInt(splitDates[2]);

	DateTime dt = new DateTime(year, month, day, 0, 0,0, DateTimeZone.UTC);
		
	return dt.getMillis();
	
    }
    


    // 1422939760000   12/03/2011 11:16 AM PST
    // 1322936160000   12/03/2011 10:16 AM PST


    private DynamicComposite getStartRange(String timeDimension, long startRange, long endRange) {
	DynamicComposite range = new DynamicComposite();
	range.add(0, timeDimension);
	range.addComponent(new Long(startRange), longSerializer, "LongType", AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL);
	return range;
    }
    private DynamicComposite getEndRange(String timeDimension, long startRange, long endRange) {
	DynamicComposite range = new DynamicComposite();
	range.add(0, timeDimension);
	range.addComponent(new Long(endRange), longSerializer, "LongType", AbstractComposite.ComponentEquality.LESS_THAN_EQUAL);
	return range;
    }
    
    private  String lookupCategoryId (String category) {
	return category;
    }

    private  String lookupCategoryTimeDimensions(String lookupCategoryId) {
	return defaultTimeDimensions;
    }
    
    
    public void increment(String category, String key, long time, int value) {
	String lookupCategoryId = lookupCategoryId(category);
	String timeDimensions = lookupCategoryTimeDimensions(lookupCategoryId);
	
	String [] splitKeys = key.split(delimiter);
	int size = splitKeys.length;
	for (int i = 1; i <= size; i++) {
	    String subtree = new String();
	    for (int j = 0; j < i; j++) {
		if (j < (i-1) ) {
		    subtree = subtree+splitKeys[j]+sDelimiter;
		}
		else {
		    subtree = subtree+splitKeys[j];
		}
	    }
	    denormalizedIncrement(lookupCategoryId, timeDimensions, subtree, time, value);
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


}
