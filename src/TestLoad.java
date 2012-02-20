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
import java.io.*;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.countandra.kafka.*;


public class TestLoad {
    private static StringBuffer buf = new StringBuffer();

    private static String usage = new String("Usage : TestLoad [[h|k]] [<filename>] init");
    
    public static void main(String[] args) {
	TestLoad nt = new TestLoad();
	
	if (args.length == 0) {
	    System.out.println(usage);
	} 
	if (args.length == 3) {
	    nt.initcassandradb();
	    
	}
	
	if (args[0].equals("h")) {
	    nt.httptest(args[1]);
	    
	} else if (args[0].equals("k")) {
	    nt.kafkatest(args[1]);
		    
	}
	else {
	    System.out.println(usage);
	}
    }    


    public void kafkatest (String fileName) {
	KafkaProducer producer = new KafkaProducer(KafkaProperties.topic, fileName);
	producer.start();
    }
    

    public void httptest(String fileName) {
	ClientBootstrap client = new ClientBootstrap(
						     new NioClientSocketChannelFactory(
										       Executors.newCachedThreadPool(),
										       Executors.newCachedThreadPool()));

	client.setPipelineFactory(new ClientPipelineFactory());

	Channel channel = null;
	HttpRequest request;
	ChannelBuffer buffer;

	try {
	    FileInputStream fstream = new FileInputStream(fileName);

	    DataInputStream in = new DataInputStream(fstream);
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String strLine;

	    long starttimestamp = System.currentTimeMillis();
	    long endtimestamp = System.currentTimeMillis();
	    
	    int recCount = 0;

	    while ((strLine = br.readLine()) != null) {
		channel = client
		    .connect(new InetSocketAddress("127.0.0.1", 8080))
		    .awaitUninterruptibly().getChannel();
		request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
						 HttpMethod.POST, "insert");
		recCount++;
		if ((recCount % 1000) == 1) {
		    endtimestamp = System.currentTimeMillis();
		    System.out.print("It took " );
		    System.out.print( endtimestamp - starttimestamp );
		    System.out.println(" ms to load 1000 records through http");
		    starttimestamp = endtimestamp;
		}
		
		buffer = ChannelBuffers.copiedBuffer(strLine,
						     Charset.defaultCharset());
		request.addHeader(
				  org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH,
				  buffer.readableBytes());
		request.setContent(buffer);
		channel.write(request).awaitUninterruptibly().getChannel()
		    .getCloseFuture().awaitUninterruptibly();
		;
	    }

	} catch (Exception e) {// Catch exception if any
	    System.err.println("Error: " + e.getMessage());
	}
	channel.getCloseFuture().awaitUninterruptibly();

	client.releaseExternalResources();

    }

    public void initcassandradb() {
	ClientBootstrap client = new ClientBootstrap(
						     new NioClientSocketChannelFactory(
										       Executors.newCachedThreadPool(),
										       Executors.newCachedThreadPool()));

	client.setPipelineFactory(new ClientPipelineFactory());

	// Connect to server, wait till connection is established, get channel
	// to write to
	Channel channel;
	HttpRequest request;
	ChannelBuffer buffer;

	channel = client.connect(new InetSocketAddress("127.0.0.1", 8080))
	    .awaitUninterruptibly().getChannel();
	request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
					 "init");

	buffer = ChannelBuffers.copiedBuffer(" ", Charset.defaultCharset());
	request.addHeader(
			  org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH,
			  buffer.readableBytes());
	request.setContent(buffer);
	channel.write(request).awaitUninterruptibly().getChannel()
	    .getCloseFuture().awaitUninterruptibly();

	client.releaseExternalResources();

    }

    private static class TestResponseHandler extends
						 SimpleChannelUpstreamHandler {
	@Override
	    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
	    throws Exception {
	    HttpResponse responce = (HttpResponse) e.getMessage();
	    buf.append(responce.getContent().toString(CharsetUtil.UTF_8));
	    super.messageReceived(ctx, e);
	}
    }

    private static class ClientPipelineFactory implements
						   ChannelPipelineFactory {

	@Override
	    public ChannelPipeline getPipeline() throws Exception {
	    ChannelPipeline pipeline = Channels.pipeline();

	    pipeline.addLast("codec", new HttpClientCodec());

	    pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));

	    pipeline.addLast("handler", new TestResponseHandler());
	    return pipeline;
	}
    }

}