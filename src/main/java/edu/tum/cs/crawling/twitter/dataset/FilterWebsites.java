package edu.tum.cs.crawling.twitter.dataset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import edu.tum.cs.crawling.twitter.server.TwitterDao;
import edu.tum.cs.util.LanguageDetection;

public class FilterWebsites {

	private static final String[] errorPageUrlPrefix = {
		"https://myaccount.nytimes.com/auth/login",
		"https://accounts.google.com/ServiceLogin",
		"https://www.facebook.com/login.php"
	};

	private static final String bom = "\u00ef\u00bb\u00bf";
	private static final Pattern htmlTag = Pattern.compile("(?m)</?[a-zA-Z][a-zA-Z0-9 \\\"=_\\-\\r\\n]*?>");
	private static final int maxBatchSize = 10000;

	private static String detectLanguage(String text) {
		String lang = "";
		try {
			Detector languageDetector = DetectorFactory.create();
			languageDetector.append(text);
			lang = languageDetector.detect();
		} catch (LangDetectException ex) {
			// ignore
		}
		return lang;
	}

	public static void main(String[] args) throws Exception {
		LanguageDetection.loadProfilesFromResources();

		int numProcessed = 0, numKept = 0;
		Connection c = TwitterDao.getConnection();
		try {
			Statement s = c.createStatement();
			s.executeUpdate("create table `WEBSITE_FILTERED` like `WEBSITE`");

			PreparedStatement si = c.prepareStatement("insert into WEBSITE_FILTERED (TWEET_ID,ORIGINAL_URL," +
					"RESOLVED_URL,STATUS_CODE,CONTENT) values (?,?,?,?,?)");
			int batchSize = 0;

			ResultSet rs = s.executeQuery("select TWEET_ID,ORIGINAL_URL,RESOLVED_URL,STATUS_CODE,CONTENT from WEBSITE" +
					" where CONTENT is not null");
			while (rs.next()) {
				long tweetId = rs.getLong(1);
				String originalUrl = rs.getString(2);
				String resolvedUrl = rs.getString(3);
				int statusCode = rs.getInt(4);
				String content = rs.getString(5);
				if ((++numProcessed % 100000) == 0)
					System.err.println("processed " + numProcessed + " websites");

				// discard known login/error pages with many duplicates
				boolean isErrorPage = false;
				for (String prefix : errorPageUrlPrefix) {
					if (resolvedUrl.startsWith(prefix)) {
						isErrorPage = true;
						break;
					}
				}
				if (isErrorPage)
					continue;

				// discard if only content is the BOM
				if (content.equals(bom))
					continue;
				// remove null bytes from UTF-16 text mistaken for single-byte encoding
				content = content.replace("\u0000", "");

				// remove HTML tags, even if they are split across line breaks
				content = htmlTag.matcher(content).replaceAll("");

				// filter by language
				if (content.isEmpty() || !detectLanguage(content).equals("en"))
					continue;

				numKept++;
				si.setLong(1, tweetId);
				si.setString(2, originalUrl);
				si.setString(3, resolvedUrl);
				si.setInt(4, statusCode);
				si.setString(5, content);
				si.addBatch();
				if (++batchSize > maxBatchSize) {
					si.executeBatch();
					batchSize = 0;
				}
			}
			if (batchSize > 0)
				si.executeBatch();
		} finally {
			c.close();
		}
		System.err.println("kept " + numKept + " of " + numProcessed + " websites");
	}

}
