package edu.tum.cs.crawling.livejournal.framework;

import java.io.IOException;

public class Crawler extends Task {

	private class Job
	{
		public String url;
		public Consumer target;
		public Object param;

		public Job(String url, Consumer target, Object param)
		{
			this.url = url;
			this.target = target;
			this.param = param;
		}

		public void done(Resource res)
		{
			if (target != null)
				target.handleResource(res, param);
		}
	}

	private int interval;
	private WarcWriter archive;
	private Fetcher fetcher;

	public Crawler(String userAgent, int interval)
	{
		this(userAgent, interval, null);
	}

	public Crawler(String userAgent, int interval, WarcWriter archive)
	{
		super();
		this.interval = interval;
		this.archive = archive;
		fetcher = new Fetcher(userAgent);
	}

	public void perform(Object rawJob)
	{
		// TODO: limit backlog of unprocessed resources

		Job job = (Job) rawJob;
		System.err.println("trying to fetch " + job.url);
		Resource res;
		try
		{
			res = fetcher.retrieveUrl(job.url);

			try
			{
				if (archive != null)
					archive.write(res);
			}
			catch (IOException ex)
			{
				System.err.println("error writing to archive: " +
						ex.getMessage());
			}

			job.done(res);
		}
		catch (IOException ex)
		{
			res = new Resource(job.url);
			res.resultCode = Resource.INVALID_RESPONSE;
			job.done(res);
		}

		System.err.println("done, waiting " + Integer.toString(interval) +
				" seconds");
		try
		{
			Thread.sleep(interval * 1000);
		}
		catch (InterruptedException ex)
		{
			if (running)
				throw new RuntimeException("crawler thread interrupted");
		}
	}

	public void addJob(String url, Consumer target, Object param)
	{
		addJob(new Job(url, target, param));
	}

}
