package com.rain.writeMina;

import java.io.IOException;
import java.net.InetSocketAddress;

import rain.core.session.IdleStatus;
import rain.filter.codec.ProtocolCodecFilter;
import rain.filter.codec.textline.TextLineCodecFactory;
import rain.filter.logging.LoggingFilter;
import rain.transport.socket.nio.NioSocketAcceptor;

public class MinaServer {
	public static void main(String[] args) {
		try {
			// 第一步，新建accepter：服务端（等待客户端连接，所以命名为acceptor）
			NioSocketAcceptor acceptor = new NioSocketAcceptor();
			// 第二步，设置Handler，需要实现IOHandler接口，用于处理消息（主要有创建、连接、接收、发送、关闭、异常、闲置7个状态回调）
			acceptor.setHandler(new MinaServerHandler());
			// 第三步，设置拦截器
			// 设置log拦截器
			acceptor.getFilterChain().addLast("log", new LoggingFilter());
			// 设定消息编码规则拦截器
			acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory()));
			acceptor.getSessionConfig().setReadBufferSize(2048);
			/*
			 * 指定了什么时候检查空闲 session。 第一个参数用于判断session是否闲置的条件
			 * 有三个状态：1.不读取也不写入时判断为闲置，2.不读取时判断为闲置，3.不写入时判断为闲置，默认为2
			 * 第二个参数表示session闲置时在10秒后调用Handler的sessionIdle方法。
			 */
			acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
			// 第四步，创建端口，等待连接，端口号2001，客户端需要连接到该端口
			acceptor.bind(new InetSocketAddress(20012));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}