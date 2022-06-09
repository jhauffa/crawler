package edu.tum.cs.crawling.facebook.server;

import java.io.File;
import java.util.logging.Logger;

import edu.tum.cs.crawling.facebook.entities.UserProfile;
import edu.tum.cs.util.LanguageDetection;
import edu.tum.cs.util.LogConfigurator;

public class ProcessArchives extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(ProcessArchives.class.getName());

	private static void processArchive(File archiveFile) throws Exception {
		WarcArchiver.Archive archive = WarcArchiver.loadPageSource(archiveFile);
		UserProfileExtractor extractor = UserProfileExtractor.createExtractor(archive.date, archive.userId,
				archive.pageSource, false);
		UserProfile profile = extractor.extractProfile();
		if (profile != null)
			FacebookDao.saveUserProfile(profile);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("usage: " + ProcessArchives.class.getName() + " <DB host> <DB name>" +
					" <DB user> <DB password> [<archive file> ...]");
			return;
		}

		LanguageDetection.loadProfilesFromResources();
		FacebookDao.initialize(args[0], args[1], args[2], args[3]);
		try {
			for (int i = 4; i < args.length; i++) {
				logger.info("processing " + args[i] + "...");
				processArchive(new File(args[i]));
			}
		} finally {
			FacebookDao.shutdown();
		}
	}

}
