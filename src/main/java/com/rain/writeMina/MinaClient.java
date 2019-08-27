package com.rain.writeMina;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import rain.core.future.ConnectFuture;
import rain.filter.codec.ProtocolCodecFilter;
import rain.filter.codec.textline.TextLineCodecFactory;
import rain.transport.socket.mynio.MyNioSocketConnector;

public class MinaClient {

	public static void main(String[] args) {

		for (int i = 0; i < 1; i++) {
			new Thread(() -> {
				try {
					newClient();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}).start();
		}
	}

	public static void newClient() throws InterruptedException {

		// 第一步，建立一个connecter
		MyNioSocketConnector connecter = new MyNioSocketConnector();
		// 第二步，设置消息处理的Handler，和服务端一模一样，实现IOHandler接口即可
		connecter.setHandler(new MinaClientHandler());
		// 第三步，设置拦截器，编码规则应该和服务端一样，即TextLineCodecFactory，除了mina自带的编码方式之外，还可以自己定义编码协议
		connecter.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory()));
		// 第四步，连接服务器，127.0.0.1代表本机ip，2001是端口号
		ConnectFuture future = connecter.connect(new InetSocketAddress("127.0.0.1", 20012));
		// 阻塞直到和服务器连接成功
		future.awaitUninterruptibly();
		while (true) {
			future.getSession().write("客户端发送消息：" + Thread.currentThread().getName());
			Thread.sleep(2000);
		}
	}

	public static void input(ConnectFuture future) {
		// 下面代码用于测试，从客户端控制台输入
		BufferedReader inputReader = null;
		try {
			inputReader = new BufferedReader(new InputStreamReader(System.in, "utf-8")); // 从控制台读取的输入内容
			String s;
			while (!(s = inputReader.readLine()).equals("exit")) {
				future.getSession().write("客户端发送消息：" + s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}