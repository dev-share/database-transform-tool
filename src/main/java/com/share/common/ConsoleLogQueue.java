package com.share.common;

import java.util.concurrent.LinkedBlockingQueue;

public class ConsoleLogQueue {
	private static class InitSingleton{
		private final static ConsoleLogQueue INSTANCE = new ConsoleLogQueue();
	}
	private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
	private ConsoleLogQueue() {}
	public final static ConsoleLogQueue getIntance(){
		return InitSingleton.INSTANCE;
	}
	public void log(String info) {
		synchronized (queue) {
			queue.offer(info);
		}
	}
	public String println() {
		String info = null;
		if(!queue.isEmpty()) {
			info = queue.poll();
		}
		return info;
	}
	public String printAll() {
		String info = "";
		while(!queue.isEmpty()) {
			info +="\\n"+ queue.poll();
		}
		return info;
	}
}
