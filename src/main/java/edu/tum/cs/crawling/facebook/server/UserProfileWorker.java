package edu.tum.cs.crawling.facebook.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.tum.cs.crawling.facebook.entities.Person;
import edu.tum.cs.crawling.facebook.entities.UserProfile;

public class UserProfileWorker implements Runnable {

	private static final Logger logger = Logger.getLogger(UserProfileWorker.class.getName());
	private static final IdBroker idBroker = IdBroker.getInstance();
	private static final WarcArchiver warcArchiver = WarcArchiver.getInstance();

	private static final BlockingQueue<File> workQueue = new LinkedBlockingQueue<File>();
	private static final AtomicInteger pendingProfiles = new AtomicInteger(0);

	@Override
	public void run() {
		while (!Thread.interrupted()) {
			File archiveFile;
			try {
				pendingProfiles.getAndIncrement();
				archiveFile = workQueue.take();
			} catch (InterruptedException ex) {
				pendingProfiles.decrementAndGet();
				break;
			}

			WarcArchiver.Archive archive = null;
			try {
				archive = WarcArchiver.loadPageSource(archiveFile);
				UserProfileExtractor extractor = UserProfileExtractor.createExtractor(archive.date, archive.userId,
						archive.pageSource, false);
				UserProfile profile = extractor.extractProfile();
				if (profile != null) {
					FacebookDao.saveUserProfile(profile);

					if (profile.getLanguage().equals("en")) {
						List<String> friendIds = new ArrayList<String>(profile.getFriends().size());
						for (Person p : profile.getFriends())
							friendIds.add(p.getId());
						idBroker.addUserIds(friendIds);
					}
				} else
					logger.warning("missing or incomplete user profile in file '" + archiveFile + "'");
				warcArchiver.setIdProcessed(archive.userId);
			} catch (Exception ex) {
				logger.log(Level.SEVERE, "error while processing profile '" + archiveFile + "'", ex);
			} finally {
				pendingProfiles.decrementAndGet();
			}
		}
	}

	public static void submitArchive(File archiveFile) {
		workQueue.add(archiveFile);
	}

	public static void submitArchives(List<File> archiveFiles) {
		workQueue.addAll(archiveFiles);
	}

	public static int getNumPendingProfiles() {
		return workQueue.size() + pendingProfiles.get();
	}

}
