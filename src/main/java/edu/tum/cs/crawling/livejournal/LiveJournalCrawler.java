package edu.tum.cs.crawling.livejournal;

import java.io.IOException;

import edu.tum.cs.crawling.livejournal.framework.Application;
import edu.tum.cs.crawling.livejournal.framework.Crawler;
import edu.tum.cs.crawling.livejournal.framework.RobotRules;
import edu.tum.cs.crawling.livejournal.framework.WarcWriter;

public class LiveJournalCrawler {

	private static final String userAgent = "JavaWebCrawler";

	private static void printUsageAndExit()
	{
		System.err.println("usage: LiveJournalCrawler seed limit");
		System.err.println("where 'seed' is a valid LJ user name\n" +
				"and 'limit' is the maximum number of users to crawl");
		System.exit(1);
	}

	public static void main(String args[])
	{
		if (args.length < 2)
			printUsageAndExit();
		String seedUser = args[0];
		int limit = 0;
		try
		{
			limit = Integer.parseInt(args[1]);
		}
		catch (NumberFormatException ex)
		{
			printUsageAndExit();
		}

		WarcWriter archive;
		try
		{
			archive = new WarcWriter("lj", userAgent);
		}
		catch (IOException ex)
		{
			System.err.println("error creating archive file: " +
					ex.getMessage());
			ex.printStackTrace();
			return;
		}

		RobotRules rules = new RobotRules(userAgent);
		rules.retrieveForDomain("www.livejournal.com");

		Crawler crawler = new Crawler(userAgent, rules.getFetchInterval(),
				archive);
		LiveJournalProcessor processor = new LiveJournalProcessor(crawler,
				seedUser, limit);

		Application app = new Application(crawler, processor, archive);
		app.run();

		processor.printStatus();
	}

}
