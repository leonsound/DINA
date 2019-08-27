package com.rain.writeMina;

import rain.core.service.IoHandlerAdapter;
import rain.core.session.IdleStatus;
import rain.core.session.IoSession;

public class MinaClientHandler extends IoHandlerAdapter {

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {

		System.out.println("客户端异常捕捉");

	}

	@Override
	public void messageSent(IoSession session, Object message) throws Exception {

		System.out.println("客户端消息发送:" + message.toString());

	}
	@Override
	public void sessionClosed(IoSession session) throws Exception {
		System.out.println("客户端session关闭");

	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
		System.out.println("客户端session闲置");
	}

	@Override

	public void sessionOpened(IoSession session) throws Exception {
		System.out.println("客户端连接成功");

	}

	@Override

	public void messageReceived(IoSession session, Object message) throws Exception {
		System.out.println("客户端接收消息：" + message.toString());

	}

}