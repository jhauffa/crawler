package edu.tum.cs.crawling.twitter.dataset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import edu.tum.cs.crawling.twitter.server.TwitterDao;
import edu.tum.cs.util.LanguageDetection;
import edu.tum.cs.util.LogConfigurator;

public class AddDetectedLanguage extends LogConfigurator {

	private static final Logger logger = Logger.getLogger(AddDetectedLanguage.class.getName());

	public static void main(String[] args) throws Exception {
		LanguageDetection.loadProfilesFromResources();

		Connection c = TwitterDao.getConnection();
		try {
			PreparedStatement psUpdateDetectedLanguage = c.prepareStatement(
					"UPDATE user SET detected_language = ? WHERE id = ?");
			PreparedStatement psSelectStatusTexts = c.prepareStatement(
					"SELECT status_text FROM tweet WHERE user_id = ? LIMIT 200");

			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("SELECT id, ignored FROM user WHERE detected_language IS NULL");

			int i = 0, nonEn = 0, unknownLanguage = 0;
			logger.info("Starting");
			while (rs.next()) {
				long userId = rs.getLong(1);
				boolean ignored = rs.getBoolean(2);

				psSelectStatusTexts.setLong(1, userId);
				ResultSet rsTweets = psSelectStatusTexts.executeQuery();
				boolean foundTweets = false;
				Detector detector = DetectorFactory.create();
				while (rsTweets.next()) {
					String statusText = rsTweets.getString(1);
					detector.append(statusText);
					foundTweets = true;
				}
				if (!foundTweets)
					continue;

				String languageCode = LanguageDetection.UNKNOWN_LANGUAGE;
				try {
					languageCode = detector.detect();
				} catch (LangDetectException e) {
					// ignore
				}
				if (!ignored && !languageCode.equalsIgnoreCase("en")) {
					if (languageCode.equalsIgnoreCase(LanguageDetection.UNKNOWN_LANGUAGE)) {
						languageCode = "-";
						unknownLanguage++;
					} else {
						nonEn++;
					}
				}

				psUpdateDetectedLanguage.setString(1, languageCode);
				psUpdateDetectedLanguage.setLong(2, userId);
				psUpdateDetectedLanguage.addBatch();

				if ((++i % 1000) == 0) {
					psUpdateDetectedLanguage.executeBatch();
					logger.info("i: " + i + " - nonEn: " + nonEn + " - unknownLanguage: " + unknownLanguage);
				}
			}
			psUpdateDetectedLanguage.executeBatch();
			logger.info("done - i: " + i + " - nonEn: " + nonEn + " - unknownLanguage: " + unknownLanguage);
		} finally {
			c.close();
		}
	}

}
