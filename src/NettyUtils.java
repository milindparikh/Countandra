import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
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
import org.jboss.netty.handler.codec.http.QueryStringDecoder;


public class NettyUtils {
    private static boolean nettyStarted = false;
    public static final  String PROCESSINSERT = "insert";
    public static final String M_CATEGORY = "c";
    public static final String M_SUBTREE = "s";
    public static final String M_TIMEPERIOD = "t";
    public static final String M_VALUE = "v";


    
    public static synchronized void startupNettyServer(int httpPort)  {
	if (nettyStarted) return;
	nettyStarted=true;
	ServerBootstrap server 
	    = new ServerBootstrap(
				  new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),	Executors.newCachedThreadPool())
				  );
	
			
	server.setPipelineFactory(new CountandraHttpServerPipelineFactory());		
	server.bind(new InetSocketAddress(httpPort));
			
    }


    private static class CountandraHttpServerPipelineFactory 
	implements ChannelPipelineFactory {
	@Override		
	    public ChannelPipeline getPipeline() throws Exception {
	    ChannelPipeline pipeline = Channels.pipeline();
	    pipeline.addLast("decoder", new HttpRequestDecoder());
	    pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
	    pipeline.addLast("encoder", new HttpResponseEncoder());
	    pipeline.addLast("handler", new CountandraHttpRequestHandler());
	    return pipeline;
	}

    }


    private static class CountandraHttpRequestHandler 
	extends SimpleChannelUpstreamHandler {
	private final StringBuilder buf = new StringBuilder();

	    public void processInsertRequest(String postContent) {
		String [] splitContents = postContent.split("&");

		String category = "";
		String subTree = "";
		String timePeriod="";
		String value="";
			    
		String [] hshValues;
			    
		for (int i = 0; i < splitContents.length; i++) {
		    hshValues = splitContents[i].split("=");
		    if (hshValues[0].equals(M_CATEGORY)) {
			category = hshValues[1];
		    }
		    else if (hshValues[0].equals(M_SUBTREE)) {
			subTree = hshValues[1];
		    }
		    else if (hshValues[0].equals(M_TIMEPERIOD)) {
			timePeriod = hshValues[1];
		    }
		    else if (hshValues[0].equals(M_VALUE)) {
			value = hshValues[1];
		    }
		}
		//		internalprocess(category, subTree,timePeriod, value);
		CountandraUtils cu = new CountandraUtils();
		cu.increment(category, subTree, Long.parseLong(timePeriod), Integer.parseInt(value));
	    }

		
	    public void internalprocess(String category, String subTree, String timePeriod, String value){
		    System.out.print(category + ":--:");
		    System.out.print(subTree + ":--:");
		    System.out.print(timePeriod + ":--:");
		    System.out.println(value);
	    }

	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

	    HttpRequest request = (HttpRequest) e.getMessage();
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());

            buf.setLength(0);

	    if (request.getMethod() == HttpMethod.GET) {
		
		buf.append("REQUEST_URI: " + request.getUri() + "\r\n\r\n");

		buf.append(CountandraUtils.processRequest(request.getUri()));
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));

		response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
		e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
	    
	    }
	    else if (request.getMethod() == HttpMethod.POST) {
		    if (request.getUri().equals(PROCESSINSERT)) {
			buf.setLength(0);
			buf.append(((HttpRequest) e.getMessage()).getContent().toString(CharsetUtil.UTF_8));
			String postContent = buf.toString();
			processInsertRequest(postContent);
			//Writing response, wait till it is completely written and close channel after that
			HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
			response.setContent(ChannelBuffers.copiedBuffer("ok", CharsetUtil.UTF_8));
			response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
			e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
		    }
		    else {
			HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_IMPLEMENTED);
			e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
		    }
		    
	    }
	    else {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_IMPLEMENTED);
		e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
	
	    }
	}
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	    super.exceptionCaught(ctx, e);		
	}

    }



}


