package org.countandra.kafka;
import java.nio.ByteBuffer;

import kafka.message.Message;


public class KafkaUtils {

    public static void startupKafkaConsumer() {
	KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
	consumerThread.start();
    }
    public static String getMessage(Message message)
    {
	ByteBuffer buffer = message.payload();
	byte [] bytes = new byte[buffer.remaining()];
	buffer.get(bytes);
	return new String(bytes);
    }
}
