package org.countandra.utils;

import java.io.*;
import java.lang.reflect.Field;

import org.apache.cassandra.service.AbstractCassandraDaemon;

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
import java.util.regex.*;

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

import org.countandra.exceptions.CountandraException;
import org.countandra.cassandra.*;

public class CountandraUtils {


    static String delimiter = "\\.";
    static String sDelimiter = ".";


    



    static DynamicCompositeSerializer dcs = new DynamicCompositeSerializer(); 
    static StringSerializer stringSerializer = StringSerializer.get();
    static LongSerializer longSerializer = LongSerializer.get();    
    static public CassandraServer server = new CassandraServer(); 
    static Pattern isNumber = Pattern.compile("[0-9]");

    public static String countandraCF = new String("DDCC");
    
    
    /* Time Dimensions to store against
       
       m = minutely
       H = hourly
       D = Daily
       M = Monthly
       Y = Yearly
       A = All Time
       
    */
    

	

    public enum TimeDimension {
	MINUTELY(10, "m", "MINUTELY"), HOURLY(20, "H", "HOURLY"), DAILY(30, "D", "DAILY"), MONTHLY(40, "M", "MONTHLY"), YEARLY(50, "Y", "YEARLY"), ALLTIME(100, "A", "ALLTIME");
	private int code;
 	private String sCode;
	private String lCode;
	
	private TimeDimension(int c, String s, String l) {
	    code = c;
	    sCode = s;
	    lCode = l;
	}
	public String getLCode() {
	    return lCode;
	}
	
	public int getCode() {
	    return code;
	}
	public String getSCode() {
	    return sCode;
	}
    }




    public enum TimePeriod {
	// THISHOUR
	// LASTHOUR
	// TODAY 
	// YESTERDAY 
	// LASTTFHOURS  -- last twenty four hours
	// THISWEEK     
	// LASTWEEK     
	// THISMONTH
	// LASTMONTH    
	// THISQ        TBD
	// LASTQ        TBD
	// THISYEAR
	// LASTYEAR
	
	THISHOUR("TH", "THISHOUR"), LASTHOUR("LH", "LASTHOUR"), LASTTFHOURS("LT", "LASTTFHOURS"), TODAY("TO", "TODAY"), YESTERDAY("YE", "YESTERDAY"), THISWEEK("TW", "THISWEEK"), LASTWEEK("LW", "LASTWEEK"), THISMONTH("TM", "THISMONTH"), LASTMONTH("LM", "LASTMONTH"), THISQ("TQ", "THISQ"), LASTQ("LQ", "LASTQ"), THISYEAR("TY", "THISYEAR"), LASTYEAR("LY", "LASTYEAR");
	    
	private String sCode;
	private String lCode;
	
	private TimePeriod( String s, String l) {

	    sCode = s;
	    lCode = l;
	}
	public String getLCode() {
	    return lCode;
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

    static HashMap <String, TimeDimension> hshReverseSupportedTimeDimensions = new HashMap<String, TimeDimension> ();
    static {
	hshReverseSupportedTimeDimensions.put("MINUTELY", TimeDimension.MINUTELY);
	hshReverseSupportedTimeDimensions.put("HOURLY", TimeDimension.HOURLY);
	hshReverseSupportedTimeDimensions.put("DAILY", TimeDimension.DAILY);
	hshReverseSupportedTimeDimensions.put("MONTHLY", TimeDimension.MONTHLY);
	hshReverseSupportedTimeDimensions.put("YEARLY", TimeDimension.YEARLY);
	hshReverseSupportedTimeDimensions.put("ALLTIME", TimeDimension.ALLTIME);
    }

    
    static HashMap <String, TimePeriod> hshSupportedTimePeriods = new HashMap<String, TimePeriod> ();
    static {
	hshSupportedTimePeriods.put("THISHOUR", TimePeriod.THISHOUR);
	hshSupportedTimePeriods.put("LASTHOUR", TimePeriod.LASTHOUR);
	hshSupportedTimePeriods.put("LASTTFHOURS", TimePeriod.LASTTFHOURS);
	hshSupportedTimePeriods.put("TODAY", TimePeriod.TODAY);
	hshSupportedTimePeriods.put("YESTERDAY", TimePeriod.YESTERDAY);
	hshSupportedTimePeriods.put("THISWEEK", TimePeriod.THISWEEK);
	hshSupportedTimePeriods.put("LASTWEEK", TimePeriod.LASTWEEK);
	hshSupportedTimePeriods.put("THISMONTH", TimePeriod.THISMONTH);
	hshSupportedTimePeriods.put("THISYEAR", TimePeriod.THISYEAR);
	hshSupportedTimePeriods.put("LASTYEAR", TimePeriod.LASTYEAR);
    }
     

    static String defaultTimeDimensions = "m,H,D,M,Y,A";


    private static final Logger          logger                 = Logger.getLogger(CountandraUtils.class);


    private static boolean nettyStarted = false;
    private static StringBuffer buf = new StringBuffer();    


    public static void setCassandraHostIp(String hostIp) {
	//	CassandraStorage.setGlobalParams(hostIp);
	//	CassandraDB.setGlobalParams(hostIp);
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



    private void denormalizedIncrement(String category, String ptimeDimensions, String denormalizedKey, long time, int value) {
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
		cs.incrementCounter(category, denormalizedKey, TimeDimension.MINUTELY.getSCode(), dtm.getMillis(), value);
		break;
	    case HOURLY:
		cs.incrementCounter(category, denormalizedKey, TimeDimension.HOURLY.getSCode(), dtH.getMillis(), value);
		break;
	    case DAILY:
		cs.incrementCounter(category, denormalizedKey, TimeDimension.DAILY.getSCode(), dtD.getMillis(), value);
		break;
	    case MONTHLY:
		cs.incrementCounter(category, denormalizedKey, TimeDimension.MONTHLY.getSCode(), dtM.getMillis(), value);
		break;
	    case YEARLY:
		cs.incrementCounter(category, denormalizedKey, TimeDimension.YEARLY.getSCode(), dtY.getMillis(), value);
		break;
	    case ALLTIME:
		cs.incrementCounter(category, denormalizedKey, TimeDimension.ALLTIME.getSCode(), 0L, value);
		break;

	    }
	}
    }



    // Make this thread safe



    // query/pti/com.amazon/TODAY/HOURLY/COUNTS
    // query/pti/com.amazon/THISMONTH/HOURLY/COUNTS
    // query/pti/com.amazon/THISMONTH/DAILY/SUMS
    // query/pti/com.amazon/10-07-2011:1800~12-08-2011:1900/MINUTELY/SQUARES


    
    public static String processRequest(String uri) throws CountandraException {
	
	if (Pattern.matches("/query/.*", uri) ) {
	    String [] splitParams = uri.split("/");
	    
	    if (splitParams.length > 3) {
		String category = splitParams[2];
		String subTree = splitParams[3];
		String period = splitParams[4];
		String timeDimension = splitParams[5];
		return runQuery(category, subTree, timeDimension, period);
	    }
	    else {
		throw new CountandraException(CountandraException.Reason.MISMATCHEDQUERY);
	    }
	}
	else {
	    throw new CountandraException(CountandraException.Reason.MALFORMEDQUERY);
	}
    }

    public static String runQuery(String category, String subTree, String timeDimension, String timePeriod) throws CountandraException{

	StringBuilder buf = new StringBuilder();


	TimeDimension td = hshReverseSupportedTimeDimensions.get(timeDimension);
	if (td!= null) {

	    CountandraUtils cu = new CountandraUtils();
	    ResultStatus result = cu.executeQuery(category, subTree, td.getSCode() , timePeriod, "Pacific");
	    if ( result instanceof QueryResult ) {

		buf.setLength(0);
		buf.append(" { \"Results\": ") ;
		buf.append( "{ \n");
		
		buf.append(" 	\"Category\": ");
		buf.append(category);
		buf.append(",\n");

		buf.append(" 	\"SubTree\": ");
		buf.append(subTree);
		buf.append(",\n");
		
		buf.append(" 	\"Time Dimension\": ");
		buf.append(timeDimension);
		buf.append(",\n");
	
		buf.append(" 	\"Time Period\": ");
		buf.append(timePeriod);
		buf.append(",\n");
		
		buf.append("	\"Data\": ");
		buf.append("\n[\n");
		
		buf.append("\n");
		QueryResult<?> qr = (QueryResult)result;
		if ( qr.get() instanceof CounterSlice ) {
		    List <HCounterColumn <DynamicComposite>> lCols = ((CounterSlice)qr.get()).getColumns();
		    for (int i = 0; i < lCols.size(); i++) {
			me.prettyprint.cassandra.model.HCounterColumnImpl  hcc =  (me.prettyprint.cassandra.model.HCounterColumnImpl ) lCols.get(i);
			DynamicComposite nameHcc = (DynamicComposite) hcc.getName();
			buf.append(hcc.getName());
			buf.append(hcc.getValue());
			buf.append("\n");
		    } 
		}
		
		buf.append("\n]\n");
		buf.append( "} ");
		buf.append( "} ");
		
		return (buf.toString());
	    }
	    else {
		throw new CountandraException(CountandraException.Reason.UNKNOWNERROR) ;
	    }
	} 
	else {
	    throw new CountandraException(CountandraException.Reason.TIMEDIMENSIONNOTSUPPORTED) ;
	}
    }


   


    
    

    public ResultStatus executeQuery(String category, String subTree, String timeDimensions, String timeQuery, String timeZone) {
	// MM-DD-YYYY:hh-mm | MM-DD-YYYY:hh-mm
	long startDateMillis = 0L;
	long endDateMillis = 0L;
	
	Matcher matcher = isNumber.matcher(timeQuery);
	if (matcher.find()) {
	    String s[] = timeQuery.split("\\~");
	    String startDate = s[0];
	    String endDate = s[1];
	    startDateMillis = getLongMillis(startDate);
	    endDateMillis = getLongMillis(endDate);
	}
	else {
	    startDateMillis = getStartMillis(timeQuery);
	    endDateMillis = getEndMillis(timeQuery);
	}
	return executeQuery(category, subTree, timeDimensions, startDateMillis, endDateMillis, timeZone);
    }


    private ResultStatus executeQuery( String category, String subtree, String timeDimension, long startTime, long endTime, String timeZone) {
	SliceCounterQuery<String, DynamicComposite> sliceCounterQuery = HFactory.createCounterSliceQuery(CassandraStorage.getCountandraKeySpace(), stringSerializer, dcs); 
	sliceCounterQuery.setColumnFamily(countandraCF);   
	sliceCounterQuery.setKey(getKey(category,subtree));   
	DynamicComposite startRange = getStartRange( timeDimension, startTime, endTime);
	DynamicComposite endRange = getEndRange( timeDimension, startTime, endTime);
	sliceCounterQuery.setColumnNames(startRange);
	sliceCounterQuery.setRange(startRange,endRange, false, 100);
	QueryResult  result = sliceCounterQuery.execute();    
	return result;
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



    
    private long getStartMillis(String timeQuery) {
	DateTime dt = new DateTime(DateTimeZone.UTC);

	
	TimePeriod tq = hshSupportedTimePeriods.get(timeQuery);
	
	if (tq != null) {
	    switch (tq) {
	    case THISHOUR:
		return dt.minusHours(1).getMillis();
	    case LASTHOUR:
		return dt.minusHours(2).getMillis();
	    case LASTTFHOURS:
		return dt.minusHours(24).getMillis();
	    case TODAY:
		return dt.minusHours(24).getMillis();
	    case YESTERDAY:
		return dt.minusHours(48).getMillis();
 	    case THISWEEK:
		return dt.minusWeeks(1).getMillis();
	    case LASTWEEK:
		return dt.minusWeeks(2).getMillis();
	    case THISMONTH:
		return dt.minusMonths(1).getMillis();
	    case LASTMONTH:
		return dt.minusMonths(2).getMillis();
	    case THISYEAR:
		return dt.minusYears(1).getMillis();
	    case LASTYEAR:
		return dt.minusYears(2).getMillis();

	    }
	    
	}
	else {
	
	    return 0L;
	}
	
	return 0;
	
    }


    private  long getEndMillis(String timeQuery) {
	DateTime dt = new DateTime(DateTimeZone.UTC);
	
	TimePeriod tq = hshSupportedTimePeriods.get(timeQuery);
	
	if (tq != null) {
	    switch (tq) {
	    case THISHOUR:
		return dt.getMillis();
	    case LASTHOUR:
		return dt.minusHours(1).getMillis();
	    case LASTTFHOURS:
		return dt.getMillis();
	    case TODAY:
		return dt.getMillis();
	    case YESTERDAY:
		return dt.minusHours(24).getMillis();
 	    case THISWEEK:
		return dt.getMillis();
	    case LASTWEEK:
		return dt.minusWeeks(1).getMillis();
	    case THISMONTH:
		return dt.getMillis();
	    case LASTMONTH:
		return dt.minusMonths(1).getMillis();
	    case THISYEAR:
		return dt.getMillis();
	    case LASTYEAR:
		return dt.minusYears(1).getMillis();

	    }
	    
	}
	else {
	
	    return 0L;
	}
	
	return 0L;
	
    }



    private String getKey(String category, String subTree) {
	return category+":"+subTree;

    }

    
    public static synchronized void initBasicDataStructures() throws IOException, Exception {

	System.out.println("Creating Countandra keyspace/s and column family");	
	CassandraDB csdb = new CassandraDB();
	csdb.addKeyspace(CassandraStorage.s_keySpace);
	

	csdb.createColumnFamily(CassandraStorage.s_keySpace, "MetaData");

	csdb.createColumnFamily(CassandraStorage.s_keySpace, countandraCF, "UTF8Type", "DynamicCompositeType (a=>AsciiType,b=>BytesType,i=>IntegerType,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType,A=>AsciiType(reversed=true),B=>BytesType(reversed=true),I=>IntegerType(reversed=true),X=>LexicalUUIDType(reversed=true),L=>LongType(reversed=true),T=>TimeUUIDType(reversed=true),S=>UTF8Type(reversed=true),U=>UUIDType(reversed=true))" ,"CounterColumnType" );
	System.out.println("Created Countandra keyspace/s and column family");	
	
	/* KeyValues */

	/*
	  KeyValue: KV
	  CompositeKeyValue: CKV
	  DynamicCompositeKeyValue: DCKV
	*/

	/*
	cs.createColumnFamily("COUNTANDRA", "KV");
	cs.createColumnFamily("COUNTANDRA", "CKV", "UTF8Type","CompositeType(UTF8Type, UTF8Type)","UTF8Type" );
	cs.createColumnFamily("COUNTANDRA", "DCKV", "UTF8Type","DynamicCompositeType (a=>AsciiType,b=>BytesType,i=>IntegerType,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType,A=>AsciiType(reversed=true),B=>BytesType(reversed=true),I=>IntegerType(reversed=true),X=>LexicalUUIDType(reversed=true),L=>LongType(reversed=true),T=>TimeUUIDType(reversed=true),S=>UTF8Type(reversed=true),U=>UUIDType(reversed=true))","UTF8Type" );	
	cs.createColumnFamily("COUNTANDRA", "DCKVI", "UTF8Type","DynamicCompositeType(p=>IntegerType )","UTF8Type" );	
	*/


	/* Disributed Counters */

	/*
	  DistributedCounters:DC
	  DistributedCompositeCounters: DCC
	  DistributedDynamicCompositeCounters: DDCC
	*/
	/*
	cs.createColumnFamily("COUNTANDRA", "DC", "UTF8Type", "UTF8Type","CounterColumnType" );
	cs.createColumnFamily("COUNTANDRA", "DCC", "UTF8Type","CompositeType(UTF8Type,UTF8Type)","CounterColumnType" );
	*/




    }



 }

