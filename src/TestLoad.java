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


public class TestLoad {
    private static StringBuffer buf = new StringBuffer();
    public static void main(String [] args) {
	TestLoad nt = new TestLoad();

	
	if (args.length == 0) {
	    System.out.println("Usage : TestLoad [init | -file <filename>]");
	}
	else if (args[0].equals("init")) {

	    nt.initcassandradb();
	}
	else if (args[0].equals("-file")) {
	    nt.httptest(args[1]);
	}
	else {
	    System.out.println("Usage : TestLoad [init | -file <filename>]");
	    
	}
	
	    

    }

    public void intetest() {
	
	ClientBootstrap client = new ClientBootstrap(
						     new NioClientSocketChannelFactory(
										       Executors.newCachedThreadPool(),
										       Executors.newCachedThreadPool()));

	client.setPipelineFactory(new ClientPipelineFactory());
	
	//Connect to server, wait till connection is established, get channel to write to
	Channel channel;
	HttpRequest request ;
	ChannelBuffer buffer ;
	
	try {

	    
	    channel = client.connect(new InetSocketAddress("127.0.0.1", 8080)).awaitUninterruptibly().getChannel();
	    request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "test");
	    buffer = ChannelBuffers.copiedBuffer(" ", Charset.defaultCharset());
	    
	    
	    request.addHeader(org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
	    request.setContent(buffer);
	    channel.write(request).awaitUninterruptibly().getChannel().getCloseFuture().awaitUninterruptibly();
	    
	    
	}
	catch (Exception e){//Catch exception if any
	    System.err.println("Error: " + e.getMessage());
	}
	
	client.releaseExternalResources();
	
    }
    

    
    public void httptest(String fileName) {
	ClientBootstrap client = new ClientBootstrap(
						     new NioClientSocketChannelFactory(
										       Executors.newCachedThreadPool(),
										       Executors.newCachedThreadPool()));
	
	client.setPipelineFactory(new ClientPipelineFactory());
	
	
	Channel channel = null;
	HttpRequest request ;
	ChannelBuffer buffer ;
	
	try {
	    FileInputStream fstream = new FileInputStream(fileName);
	    
	    DataInputStream in = new DataInputStream(fstream);
	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String strLine;
	    
	    long starttimestamp = System.currentTimeMillis();
	    int recCount = 0;
	    System.out.println(starttimestamp/1000);
	    while ((strLine = br.readLine()) != null)   {
		channel = client.connect(new InetSocketAddress("127.0.0.1", 8080)).awaitUninterruptibly().getChannel();
		request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "insert");
		recCount++;
		buffer = ChannelBuffers.copiedBuffer(strLine, Charset.defaultCharset());
		request.addHeader(org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
		request.setContent(buffer);
		channel.write(request).awaitUninterruptibly().getChannel().getCloseFuture().awaitUninterruptibly();;
	    }
		    
	    System.out.print("Wrote ") ;
	    System.out.print(recCount);
	    System.out.print(" records ") ;
	    System.out.print(" in ") ;
	    System.out.print( (System.currentTimeMillis() - starttimestamp) / 1000);
	    System.out.print(" seconds ") ;
	    
	}
	catch (Exception e){//Catch exception if any
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

		//Connect to server, wait till connection is established, get channel to write to
		Channel channel;
		HttpRequest request ;
		ChannelBuffer buffer ;
		

		channel = client.connect(new InetSocketAddress("127.0.0.1", 8080)).awaitUninterruptibly().getChannel();
		request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "init");

		
		buffer = ChannelBuffers.copiedBuffer(" ", Charset.defaultCharset());
		request.addHeader(org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
		request.setContent(buffer);
		channel.write(request).awaitUninterruptibly().getChannel().getCloseFuture().awaitUninterruptibly();
		    
		client.releaseExternalResources();
		
    }

    private static class TestResponseHandler extends SimpleChannelUpstreamHandler {
	@Override
	    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
	    HttpResponse responce = (HttpResponse) e.getMessage();
	    buf.append(responce.getContent().toString(CharsetUtil.UTF_8));
	    super.messageReceived(ctx, e);
	}
    }

	private static class ClientPipelineFactory implements ChannelPipelineFactory {

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