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


// LoadFile <filename> <hostname> <port>

public class LoadFile {

    private static StringBuffer buf = new StringBuffer();
    private static int port = 8080;
    private static String fileName = "textfile.txt";
    private static String hostName = "127.0.0.1";
    private static String PROCESSINSERT = "insert";
    
    
    public static void main(String [] args) {

	if (args.length == 1) {
	    fileName = args[0];
	}
 	else if (args.length == 2) {
	    fileName = args[0];

	}

	else if (args.length == 2) {
	    fileName = args[0];
	    hostName = args[1];
	    port = Integer.parseInt(args[2]);
	}
	
	
	LoadFile lf = new LoadFile();
	
	lf.loadFile();
    }

	public void loadFile() {
		ClientBootstrap client = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		client.setPipelineFactory(new ClientPipelineFactory());

		Channel channel;
		HttpRequest request ;
		ChannelBuffer buffer ;
		
		try {
		    FileInputStream fstream = new FileInputStream(fileName);
		    
		    DataInputStream in = new DataInputStream(fstream);
		    BufferedReader br = new BufferedReader(new InputStreamReader(in));
		    String strLine;

		    while ((strLine = br.readLine()) != null)   {
			channel = client.connect(new InetSocketAddress(hostName, port)).awaitUninterruptibly().getChannel();
			request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, PROCESSINSERT);
			buffer = ChannelBuffers.copiedBuffer(strLine, Charset.defaultCharset());
		    

			request.addHeader(org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH, buffer.readableBytes());
			request.setContent(buffer);
			channel.write(request).awaitUninterruptibly().getChannel().getCloseFuture().awaitUninterruptibly();
		    }
		    
		}
		catch (Exception e){//Catch exception if any
		    System.err.println("Error: " + e.getMessage());
		}
		    
		client.releaseExternalResources();
		

		System.out.println("got "+ buf.toString());
	}

    private static class LoadFileResponseHandler extends SimpleChannelUpstreamHandler {
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
			pipeline.addLast("handler", new LoadFileResponseHandler());
			return pipeline;
		}
	}


}