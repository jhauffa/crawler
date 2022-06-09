package edu.tum.cs.crawling.livejournal.framework;

public class Application {

	private class ShutdownHookThread extends Thread
	{
		@Override public void run()
		{
			if (processor.isRunning())
				processor.abort(false);
			if (crawler.isRunning())
				crawler.abort(false);
		}
	}

	private Task crawler;
	private Task processor;

	public Application(Task crawler, Task processor)
	{
		this(crawler, processor, null);
	}

	public Application(Task crawler, Task processor, WarcWriter archive)
	{
		this.crawler = crawler;
		this.processor = processor;

		Runtime.getRuntime().addShutdownHook(new ShutdownHookThread());
	}

	public void run()
	{
		processor.start();
		crawler.start();

		try
		{
			crawler.join();
			processor.terminate();
			processor.join();
		}
		catch (InterruptedException ex)
		{
			System.err.println("interrupted while waiting for thread");
			ex.printStackTrace();
			System.exit(2);
		}
	}

}
