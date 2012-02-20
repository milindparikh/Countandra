package org.countandra.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.countandra.utils.*;

public class KafkaConsumer extends Thread
{
  private final ConsumerConnector consumer;
  private final String topic;
  
  public KafkaConsumer(String topic)
  {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig());
    this.topic = topic;
  }

  private static ConsumerConfig createConsumerConfig()
  {
    Properties props = new Properties();
    props.put("zk.connect", KafkaProperties.zkConnect);
    props.put("groupid", KafkaProperties.groupId);
    props.put("zk.sessiontimeout.ms", "400");
    props.put("zk.synctime.ms", "200");
    props.put("autocommit.interval.ms", "1000");

    return new ConsumerConfig(props);

  }
 
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaMessageStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaMessageStream<Message> stream =  consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    String str;
    int cnt = 0;
    long starttimestamp = System.currentTimeMillis();
    long endtimestamp = System.currentTimeMillis();

    CountandraUtils.startBatch();
    
    while(it.hasNext()) {
	cnt++;
	if ( (cnt % CountandraUtils.BATCH_SIZE) == 1)  {
	    CountandraUtils.finishBatch();
	    endtimestamp = System.currentTimeMillis();
 	    System.out.print("It took " );
	    System.out.print( endtimestamp - starttimestamp );
	    System.out.print(" ms to insert ");
	    System.out.print(CountandraUtils.BATCH_SIZE);
	    System.out.println(" records through kafka");

	    CountandraUtils.startBatch();
	}
	starttimestamp = endtimestamp;
	str = KafkaUtils.getMessage(it.next());
	CountandraUtils.processInsertRequest(str);
    }


  }
}
