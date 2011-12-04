import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
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


public class NettyUtils {
    private static boolean nettyStarted = false;
    private static StringBuffer buf = new StringBuffer();    
    
    public static synchronized void startupNettyServer()  {
	if (nettyStarted) return;
	nettyStarted=true;

	ServerBootstrap server = new ServerBootstrap(
						     new NioServerSocketChannelFactory(
												       Executors.newCachedThreadPool(),	
												       Executors.newCachedThreadPool()));
			
	server.setPipelineFactory(new CountandraHttpServerPipelineFactory());		
	server.bind(new InetSocketAddress(8080));
			
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

	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
	    //Processing request, here is the problem. The string from the content is empty.
	    buf.append(((HttpRequest) e.getMessage()).getContent().toString(CharsetUtil.UTF_8));
	    //Writing response, wait till it is completely written and close channel after that
	    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
	    response.setContent(ChannelBuffers.copiedBuffer("pong", CharsetUtil.UTF_8));
	    response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
	    e.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
	
	}
	
	
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	    super.exceptionCaught(ctx, e);		
	}

    }
}


