import java.io.*;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import java.util.Properties;

public class KafkaProducer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private final Properties props = new Properties();
    private final String fileName ;
    
    public KafkaProducer(String topic, String fileName)
  {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("zk.connect", "localhost:2181");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
    this.fileName = fileName;
    
  }
  
  public void run() {
    int messageNo = 1;
    long starttimestamp = System.currentTimeMillis();
    long endtimestamp = System.currentTimeMillis();
	try {
    FileInputStream fstream = new FileInputStream(fileName);
    
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String strLine;
    starttimestamp = System.currentTimeMillis();
    
    while((strLine = br.readLine()) != null)
    {

	producer.send(new ProducerData<Integer, String>(topic,strLine));
	messageNo++;


    }
    endtimestamp = System.currentTimeMillis();
    System.out.print("It took " );
    System.out.print( endtimestamp - starttimestamp );
    System.out.print(" ms to load ");
    System.out.print(messageNo);
    System.out.println("  records through kafka");
	
	
	
	
	
	} catch (Exception e) {// Catch exception if any
	    System.err.println("Error: " + e.getMessage());
	}
  }
}

