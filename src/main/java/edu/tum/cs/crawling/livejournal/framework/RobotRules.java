package edu.tum.cs.crawling.livejournal.framework;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

public class RobotRules {

	private static final int defaultFetchInterval = 1;	// seconds

	private String userAgent;
	private int interval;

	public RobotRules(String userAgent)
	{
		this.userAgent = userAgent;
		interval = defaultFetchInterval;
	}

	public void retrieveForDomain(String domain)
	{
		try
		{
			Fetcher fetcher = new Fetcher(userAgent);
			Resource res =
				fetcher.retrieveUrl("http://" + domain + "/robots.txt");

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(res.content), "US-ASCII"));
			String effectiveUserAgent = userAgent.toLowerCase();
			String currentUserAgent = "";
			int intervalAll = 0, intervalUserAgent = 0;
			String line;
			while ((line = reader.readLine()) != null)
			{
				if (line.isEmpty())	// record delimiter
				{
					currentUserAgent = "";
					continue;
				}

				// strip comments
				if (line.charAt(0) == '#')
					continue;
				String[] parts = line.split("#");

				// extract key and value
				int pos = parts[0].indexOf(':');
				if (pos < 0)
					continue;
				String key = parts[0].substring(0, pos);
				key = key.trim().toLowerCase();
				if (key.length() == 0)
					continue;
				String value = parts[0].substring(pos + 1);
				value = value.trim();

				if (key.equals("user-agent"))
					currentUserAgent = value.toLowerCase();
				else if (key.equals("crawl-delay"))
				{
					if (currentUserAgent.equals(effectiveUserAgent))
						intervalUserAgent = Integer.parseInt(value);
					else if (currentUserAgent.equals("*"))
						intervalAll = Integer.parseInt(value);
				}
			}

			if (intervalUserAgent > 0)
				interval = intervalUserAgent;
			else if (intervalAll > 0)
				interval = intervalAll;
		}
		catch (IOException ex)
		{
			System.err.println("error retrieving robots.txt, using defaults");
		}
	}

	public int getFetchInterval()
	{
		return interval;
	}

}
