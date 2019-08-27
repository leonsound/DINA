package com.rain.writeMina;

import java.util.concurrent.ConcurrentLinkedQueue;

import rain.core.service.IoHandlerAdapter;
import rain.core.session.IdleStatus;
import rain.core.session.IoSession;

public class MinaServerHandler extends IoHandlerAdapter {

	// Mina会自动调用这些方法，具体要在什么时候做什么需要自行实现

	// 发生异常回调，可打印异常

	@Override

	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		System.out.println("服务端捕捉：" + cause);

	}

	// 接收到消息时回调

	@Override

	public void messageReceived(IoSession session, Object message) throws Exception {

		System.out.println(Thread.currentThread().getName() + "=服务端消息接收：" + message.toString());
		/*
		 * if (message.toString().trim().equalsIgnoreCase("quit")) { session.close();
		 * return; }
		 */
		// MsgQueue.offer(new MsgHandler(session, message));

	} // 发送消息成功时调用，注意发送消息不能用这个方法，而是用session.write();

	@Override

	public void messageSent(IoSession session, Object message) throws Exception {

		System.out.println("服务端消息发送:" + message.toString());

	} // 连接断开时调用

	@Override

	public void sessionClosed(IoSession session) throws Exception {

		System.out.println("服务端session关闭");

	}

//连接创建时调用

	@Override

	public void sessionCreated(IoSession session) throws Exception {

		System.out.println("服务端session创建");

	}

//连接闲置时调用，闲置状态通过setIdleTime第一个参数判断，调用频率通过setIdleTime第二个参数设置，这里是10s一次

	@Override

	public void sessionIdle(IoSession session, IdleStatus status) throws Exception {

		System.out.println("服务端session闲置");

	}

	// 连接成功时回调

	@Override

	public void sessionOpened(IoSession session) throws Exception {

		System.out.println("服务端连接成功");

	}

}