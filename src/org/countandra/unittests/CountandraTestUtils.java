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
package org.countandra.unittests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.jboss.netty.handler.codec.http.DefaultHttpRequest;

import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import org.jboss.netty.handler.codec.http.HttpResponse;

import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountandraTestUtils {

	private static StringBuffer buf = new StringBuffer();
	private static Logger log = LoggerFactory
			.getLogger(CountandraTestUtils.class);
	private static ClientBootstrap client = new ClientBootstrap(
			new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool()));

	private static int httpPort = 8080;

	public static synchronized void setGlobalParams(int port) {
		httpPort = port;
	}

	public static synchronized void setPipeLineFactory() {
		client.setPipelineFactory(new ClientPipelineFactory());
	}

	public static void close() {
		client.releaseExternalResources();
	}

	public static void insertData(String content) {
		Channel channel = null;
		HttpRequest request;
		ChannelBuffer buffer;
		try {
			channel = client
					.connect(new InetSocketAddress("localhost", httpPort))
					.awaitUninterruptibly().getChannel();
			request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
					HttpMethod.POST, "insert");
			buffer = ChannelBuffers.copiedBuffer(content,
					Charset.defaultCharset());
			request.addHeader(
					org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH,
					buffer.readableBytes());
			request.setContent(buffer);
			channel.write(request).awaitUninterruptibly().getChannel()
					.getCloseFuture().awaitUninterruptibly();

		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		channel.getCloseFuture().awaitUninterruptibly();
	}

	public static Long httpGet(String query) {
		URL url;
		try {
			url = new URL(query);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");

			conn.connect();
			InputStream in = conn.getInputStream();
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			String jsonText = reader.readLine();
			JSONParser parser = new JSONParser();
			ContainerFactory containerFactory = new ContainerFactory() {
				public List creatArrayContainer() {
					return new LinkedList();
				}

				public Map createObjectContainer() {
					return new LinkedHashMap();
				}

			};

			try {
				Map json = (Map) parser.parse(jsonText, containerFactory);
				Iterator iter = json.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry result = (Map.Entry) iter.next();
					if (result.getKey().equals("Results")) {
						LinkedHashMap resultHashMap = (LinkedHashMap) result
								.getValue();
						LinkedHashMap dataHashMap = (LinkedHashMap) resultHashMap
								.get("Data");
						Collection values = dataHashMap.values();
						Iterator valueItr = values.iterator();
						Long sum = 0L;
						while (valueItr.hasNext()) {
							sum = +(Long) valueItr.next();
						}

						return sum;
					} else {
						return 0L;
					}
				}
			} catch (ParseException pe) {
				System.out.println(pe);
			}

			conn.disconnect();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0L;

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
