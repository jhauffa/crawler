package edu.tum.cs.crawling.twitter.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.tum.cs.util.LogConfigurator;

public class TwitterDumperServer extends LogConfigurator {

	/** Thread/Runnable that accepts client requests */
	private static class NetworkService implements Runnable {
		private final ServerSocket serverSocket;
		private final ExecutorService pool;

		public NetworkService(ExecutorService pool, ServerSocket serverSocket) {
			this.serverSocket = serverSocket;
			this.pool = pool;
		}

		@Override
		public void run() {
			try {
				// Wait for client requests in infinite loop. SIGINT causes the ServerSocket to be shut down, which in
				// turn causes ServerSocket#accept to throw an IOException that breaks out of the loop.
				while (!Thread.interrupted()) {
					Socket cs = serverSocket.accept(); // wait for client request

					// start handler thread for processing the client request
					pool.execute(new TwitterDumperServerHandler(cs, TwitterDumperServer.addIdsToWaitingList));
				}
			} catch (IOException ex) {
				// ignore
			}
		}
	}

	public static final int port = 3141;

	private static boolean addIdsToWaitingList = true;
	private static Thread t1;

	public static void terminate() {
		t1.interrupt();
	}

	/**
	 * Server listens on port 3141, each client request is handled by a thread from a thread pool. Server can be stopped
	 * by sending SIGINT (Ctrl+C).
	 */
	public static void main(String[] args) throws IOException {
		if (args.length > 0)
			addIdsToWaitingList = Boolean.parseBoolean(args[0]);

		final ExecutorService pool = Executors.newCachedThreadPool();
		final ServerSocket serverSocket = new ServerSocket(port);

		t1 = new Thread(new NetworkService(pool, serverSocket));
		t1.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("terminating server");
				pool.shutdown();
				try {
					pool.awaitTermination(4L, TimeUnit.SECONDS);
					if (!serverSocket.isClosed())
						serverSocket.close();
				} catch (IOException e) {
					// ignore
				} catch (InterruptedException e) {
					// ignore
				}
			}
		});
	}

}
