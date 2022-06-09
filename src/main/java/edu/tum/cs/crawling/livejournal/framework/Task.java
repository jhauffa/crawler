package edu.tum.cs.crawling.livejournal.framework;

import java.util.LinkedList;

public abstract class Task extends Thread {

	private LinkedList<Object> jobs;
	protected boolean running;
	private boolean terminating;

	public Task()
	{
		jobs = new LinkedList<Object>();
		terminating = false;
		running = true;
	}

	private Object getNextJob()
	{
		synchronized (jobs)
		{
			while (jobs.isEmpty())
			{
				if (terminating)
					running = false;
				if (!running)
					return null;

				try
				{
					jobs.wait();
				}
				catch (InterruptedException ex)
				{
					// ignore
				}
			}
			return jobs.removeFirst();
		}
	}

	public abstract void perform(Object job);

	public void run()
	{
		while (running)
		{
			Object job = getNextJob();
			if (job == null)
				continue;
			perform(job);
		}
		System.err.println("left task run loop");
	}

	public void abort(boolean force)
	{
		System.err.println("abort called");
		synchronized (jobs)
		{
			running = false;
			if (force)
				interrupt();
			else
				jobs.notify();
		}
	}

	public void terminate()
	{
		System.err.println("terminate called");
		synchronized (jobs)
		{
			terminating = true;
			jobs.notify();
		}
	}

	protected void addJob(Object job)
	{
		synchronized (jobs)
		{
			jobs.add(job);
			jobs.notify();
		}
	}

	public void printStatus()
	{
		synchronized (jobs)
		{
			System.out.println(jobs.size() + " jobs in queue");
		}
	}

	public boolean isRunning()
	{
		return running;
	}

}
