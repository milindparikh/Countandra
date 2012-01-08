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
import org.countandra.utils.CountandraUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;

public class CassandraStorage {
	private static final int MAX_COLUMNS = 1000;
	static public CassandraServer server = new CassandraServer();

	static Cluster myCluster = null;
	static ConfigurableConsistencyLevel ccl = null;
	static Keyspace ksp = null;

	static String s_hostIp = new String("localhost:9160");
	static String s_consistencyLevel = new String("ONE");

	static String s_clusterName = new String("test-cluster");
	public static String s_keySpace = "COUNTANDRA";

	static StringSerializer stringSerializer = StringSerializer.get();
	static LongSerializer longSerializer = LongSerializer.get();
	static DynamicCompositeSerializer dcs = new DynamicCompositeSerializer();

	private static Logger log = LoggerFactory.getLogger(CassandraStorage.class);

	static {
		// setGlobalParams(s_clusterName, s_hostIp,
		// s_consistencyLevel,s_keySpace);

	}

	public static synchronized void setGlobalParams(String hostIp) {
		setGlobalParams(s_clusterName, hostIp, s_consistencyLevel, s_keySpace);

	}

	public static synchronized void setGlobalParams(String hostIp,
			String consistencyLevel) {
		setGlobalParams(s_clusterName, hostIp, consistencyLevel, s_keySpace);

	}

	public static synchronized void setGlobalParams(String clusterName,
			String hostIp, String consistencyLevel, String keySpace) {

		s_hostIp = hostIp;
		s_clusterName = clusterName;
		s_consistencyLevel = consistencyLevel;
		s_keySpace = keySpace;

		setCountandraConsistencyLevel(consistencyLevel);
		setCountandraCluster(clusterName, hostIp);
		setCountandraKeySpace(keySpace);

	}

	// aka ONE
	public static void setCountandraConsistencyLevel(String consistencyLevel) {
		ccl = new ConfigurableConsistencyLevel();
		ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
	}

	// aka testcluster, localhost:9160

	public static void setCountandraCluster(String clusterName, String hostIp) {
		myCluster = HFactory.getOrCreateCluster(clusterName, hostIp);
	}

	public static Cluster getCountandraCluster() {
		return myCluster;
	}

	public static Keyspace getCountandraKeySpace() {
		return ksp;
	}

	// aka COUNTANDRA

	public static void setCountandraKeySpace(String keySpace) {
		ksp = HFactory.createKeyspace(keySpace, myCluster, ccl);
	}

	public CassandraStorage() {

	}

	/*
	 * Set's thread local value on the server.
	 */
	/*
	 * public void setKeyspace(String keyspace) throws Exception {
	 * server.set_keyspace(keyspace); }
	 */

	public void addKeyspace(String keyspace) throws Exception {

		List<CfDef> cfDefList = new ArrayList<CfDef>();
		KsDef ksDef = new KsDef(keyspace,
				"org.apache.cassandra.locator.SimpleStrategy", cfDefList);
		ksDef.putToStrategy_options("replication_factor", "1");
		server.system_add_keyspace(ksDef);
	}

	/*
	 * public void dropColumnFamily(String columnFamily) throws Exception {
	 * server.system_drop_column_family(columnFamily); }
	 * 
	 * public void dropKeyspace(String keyspace) throws Exception {
	 * server.system_drop_keyspace(keyspace); }
	 */

	public void createColumnFamily(String keyspace, String columnFamilyName)
			throws Exception {
		this.createColumnFamily(keyspace, columnFamilyName, "UTF8Type",
				"UTF8Type", "UTF8Type");
	}

	public void createColumnFamily(String keyspace, String columnFamilyName,
			String keyValidationClass, String comparatorType,
			String defaultValidationClass) throws Exception {

		CfDef columnFamily = new CfDef(keyspace, columnFamilyName);
		columnFamily.setKey_validation_class(keyValidationClass);
		columnFamily.setComparator_type(comparatorType);
		columnFamily.setDefault_validation_class(defaultValidationClass);
		server.system_add_column_family(columnFamily);
	}

	public void incrementCounter(String rowKey, String columnKey,
			String timeDimension, long time, int value) {

		try {
			Mutator<String> m = HFactory.createMutator(ksp, stringSerializer);
			DynamicComposite dcolKey = new DynamicComposite();
			dcolKey.addComponent(timeDimension, StringSerializer.get());
			dcolKey.addComponent(time, LongSerializer.get());
			m.addCounter(rowKey + ":" + columnKey + ":COUNTS",
					CountandraUtils.countandraCF, HFactory
							.createCounterColumn(dcolKey, (long) 1,
									new DynamicCompositeSerializer()));

			m.addCounter(rowKey + ":" + columnKey + ":SUMS",
					CountandraUtils.countandraCF, HFactory.createCounterColumn(
							dcolKey, (long) value,
							new DynamicCompositeSerializer()));

			m.addCounter(
					rowKey + ":" + columnKey + ":SQUARES",
					CountandraUtils.countandraCF,
					HFactory.createCounterColumn(dcolKey, (long) value
							* (long) value, new DynamicCompositeSerializer()));

			m.execute();
			// System.out.println("after execute insert");

		} catch (Exception e) {
			System.out.println(e);

		}

	}

}