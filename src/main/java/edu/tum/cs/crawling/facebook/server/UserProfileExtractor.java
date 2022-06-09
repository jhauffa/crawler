package edu.tum.cs.crawling.facebook.server;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.jaxen.JaxenException;
import org.jaxen.XPath;
import org.jaxen.dom.DOMXPath;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import edu.tum.cs.crawling.facebook.entities.UserProfile;

public abstract class UserProfileExtractor {

	protected static final Logger logger = Logger.getLogger(UserProfileExtractor.class.getName());

	private final HtmlCleaner cleaner = new HtmlCleaner();
	private final Map<String, XPath> xpCache = new HashMap<String, XPath>();

	private final Map<String, String> pageSource;
	private final boolean ignoreErrors;

	public UserProfileExtractor(Map<String, String> pageSource, boolean ignoreErrors) {
		this.pageSource = pageSource;
		this.ignoreErrors = ignoreErrors;
		cleaner.getProperties().setDeserializeEntities(true);
	}

	private XPath prepareXPath(String path) throws JaxenException {
		XPath xp = xpCache.get(path);
		if (xp == null) {
			xp = new DOMXPath(path);
			xpCache.put(path, xp);
		}
		return xp;
	}

	protected Element findElement(Node parent, String path) {
		try {
			XPath xp = prepareXPath(path);
			return (Element) xp.selectSingleNode(parent);
		} catch (JaxenException ex) {
			throw new RuntimeException("XPath error", ex);
		}
	}

	protected List<Element> findElements(Node parent, String path) {
		try {
			XPath xp = prepareXPath(path);
			return (List<Element>) xp.selectNodes(parent);
		} catch (JaxenException ex) {
			throw new RuntimeException("XPath error", ex);
		}
	}

	protected Document fetchPage(String url) {
		String source = pageSource.get(url);
		if (source == null) {
			if (ignoreErrors) {
				logger.warning("URL '" + url + "' not in archive, returning empty document");
				source = "<html></html>";
			} else
				throw new RuntimeException("URL '" + url + "' not in archive");
		}

		// build DOM tree and convert HTML entities to Unicode characters (except in attribute values!)
		TagNode root = cleaner.clean(source);
		Document doc;
		try {
			doc = new DomSerializer(cleaner.getProperties(), false).createDOM(root);
		} catch (ParserConfigurationException ex) {
			throw new RuntimeException("error creating parser for URL '" + url + "'", ex);
		}
		return doc;
	}

	public abstract UserProfile extractProfile();

	public static UserProfileExtractor createExtractor(Date profileDate, String userId,
			Map<String, String> pageSource, boolean ignoreErrors) {
		// when re-crawling, select the appropriate extractor depending on the crawl date
		return new UserProfileExtractorCurrent(userId, pageSource, ignoreErrors);
	}

}
