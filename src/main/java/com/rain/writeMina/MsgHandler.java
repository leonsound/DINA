package com.rain.writeMina;

import rain.core.session.IoSession;

public class MsgHandler implements Runnable {
	private IoSession session;
	private Object message;

	public static volatile boolean OVER = false;

	public MsgHandler() {
	}

	public MsgHandler(IoSession session, Object message) {
		super();
		this.session = session;
		this.message = message;
	}

	@Override
	public void run() {
		OVER = false;
		System.out.println(Thread.currentThread().getName() + "处理接收：" + message.toString());
		session.write("回复消息：" + message);
		OVER = true;
		notifyAll();
	}

}
