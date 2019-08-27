package com.rain.writeMina;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MsgQueue {
	final static ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<Runnable>();
	final static Executor pool = Executors.newFixedThreadPool(10);
	final static Object lock = new Object();
	static {
		new Thread(() -> {
			for (;;) {
				//System.out.println("遍历...queue "+queue.size());
				while (!queue.isEmpty()) {
					//if (MsgHandler.OVER) {
						pool.execute(queue.remove());
					//} 
					/*
					 * else { synchronized (lock) { try { lock.wait(); } catch (InterruptedException
					 * e) { e.printStackTrace(); } } }
					 */
				}
			}

		}).start();
	}

	public static void offer(Runnable r) {
		System.out.println("offer...queue "+queue.size());
		queue.offer(r);
	}

}
