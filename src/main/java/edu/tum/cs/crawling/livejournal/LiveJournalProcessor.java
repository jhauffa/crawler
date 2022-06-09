package edu.tum.cs.crawling.livejournal;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.tum.cs.crawling.livejournal.framework.Consumer;
import edu.tum.cs.crawling.livejournal.framework.Crawler;
import edu.tum.cs.crawling.livejournal.framework.Resource;
import edu.tum.cs.crawling.livejournal.framework.Task;

public class LiveJournalProcessor extends Task implements Consumer {

	private class Job {
		public Resource res;
		public String tag;

		public Job(Resource res, String tag)
		{
			this.res = res;
			this.tag = tag;
		}
	}

	private static final String tagProfile = "profile";
	private static final String tagFriends = "friends";

	private Crawler crawler;
	private int limit;
	private HashSet<String> knownUsers;
	private HashMap<String, Integer> profileCategories;
	private Pattern xmlTagPattern;

	public LiveJournalProcessor(Crawler crawler, String seedUser, int limit)
	{
		super();
		this.crawler = crawler;
		this.limit = limit;

		knownUsers = new HashSet<String>();
		profileCategories = new HashMap<String, Integer>();
		xmlTagPattern = Pattern.compile("<([a-zA-Z:]+)(>| )");

		fetchUserData(seedUser);
	}

	public void perform(Object rawJob)
	{
		System.err.println("got job");
		Job job = (Job) rawJob;
		if (job.res.content == null)
			return;

		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new InputStreamReader(
				new ByteArrayInputStream(job.res.content), "US-ASCII"));
			if (job.tag == tagProfile)
				parseFoaf(reader);
			else if (job.tag == tagFriends)
				parseFriendList(reader);
			else
				System.err.println("unknown tag");
		}
		catch (IOException ex)
		{
			System.err.println("error processing " + job.res.url + ": " +
					ex.getMessage());
		}
	}

	private void parseFoaf(BufferedReader reader) throws IOException
	{
		// TODO: proper XML parsing, get new contacts via foaf:knows and
		//   foaf:nick
		String line;
		while ((line = reader.readLine()) != null)
		{
			// System.err.println(line);
			Matcher m = xmlTagPattern.matcher(line);
			while (m.find())
			{
				String tag = m.group(1);
				Integer count = profileCategories.get(tag);
				if (count == null)
					count = 1;
				else
					count++;
				profileCategories.put(tag, count);
			}
		}
	}

	private void parseFriendList(BufferedReader reader) throws IOException
	{
		String line;
		while ((line = reader.readLine()) != null)
		{
			if (knownUsers.size() >= limit)
			{
				System.err.println("terminating crawler");
				crawler.terminate();
				// TODO: terminate if no more users available (add new users
				//   to work queue)
				return;
			}

			if (line.isEmpty() || (line.charAt(0) == '#'))
				continue;
			String userName = line.substring(2);
			if (!knownUsers.contains(userName))
			{
				knownUsers.add(userName);
				fetchUserData(userName);
			}
		}
	}

	private void fetchUserData(String user)
	{
		crawler.addJob("http://" + user + ".livejournal.com/data/foaf",
				this, tagProfile);
		crawler.addJob("http://www.livejournal.com/misc/fdata.bml?user=" + user,
				this, tagFriends);
	}

	public void handleResource(Resource res, Object param)
	{
		System.err.println("resource callback");
		addJob(new Job(res, (String) param));
	}

	public void printStatus()
	{
		super.printStatus();
		System.out.println(profileCategories.size() + " distinct tags");
		for (Map.Entry<String, Integer> e : profileCategories.entrySet())
			System.out.println(e.getKey() + " = " + e.getValue());
	}

}
