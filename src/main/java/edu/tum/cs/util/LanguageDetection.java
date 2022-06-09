package edu.tum.cs.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.cybozu.labs.langdetect.DetectorFactory;

public class LanguageDetection {

	private static final Logger logger = Logger.getLogger(LanguageDetection.class.getName());

	/** string returned by Detector#detect if the language could not be identified */
	public static final String UNKNOWN_LANGUAGE = "unknown";

	private static boolean profilesLoaded = false;

	public static void loadProfilesFromResources() {
		if (profilesLoaded)
			return;

		try {
			// Java cannot enumerate resources in a JAR, so we have to keep a list of language profiles.
			final String[] profiles = { "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi",
					"fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr",
					"ne", "nl", "no", "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te",
					"th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw" };
			List<String> profileData = new ArrayList<String>(profiles.length);
			for (String profile : profiles) {
				InputStream is = LanguageDetection.class.getResourceAsStream("/langprofiles/" + profile);
				try {
					profileData.add(IOUtils.toString(is, "UTF-8"));
				} finally {
					is.close();
				}
			}
			DetectorFactory.loadProfile(profileData);
			profilesLoaded = true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "error loading language profiles", e);
		}
	}

}
