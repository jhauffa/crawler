package edu.tum.cs.crawling.facebook.server;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Pattern;

import net.arnx.jsonic.JSON;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import edu.tum.cs.crawling.facebook.entities.*;

public class UserProfileExtractorCurrent extends UserProfileExtractor {

	private final String userId;

	public UserProfileExtractorCurrent(String userId, Map<String, String> pageSource, boolean ignoreErrors) {
		super(pageSource, ignoreErrors);
		this.userId = userId;
	}

	/*
	private static void dumpElement(Element elem) {
		Document document = elem.getOwnerDocument();
		DOMImplementationLS domImplLS = (DOMImplementationLS) document.getImplementation();
		LSSerializer serializer = domImplLS.createLSSerializer();
		logger.info(serializer.writeToString(elem));
	}
	*/

	private Document fetchPageByName(String page) {
		String url;
		if (userId.matches("[0-9]+"))	// numeric user ID
			url = "https://m.facebook.com/profile.php?v=" + page + "&id=" + userId + "&refid=17";
		else
			url = "https://m.facebook.com/" + userId + "?v=" + page + "&refid=17";
		return fetchPage(url);
	}

	private static String getIdFromUrl(String url) {
		int start, end;
		start = url.indexOf("profile.php?id=");
		if (start != -1) {
			end = url.indexOf('&');
			if (end != -1)
				return url.substring(start + 15, end);
			return url.substring(start + 15);
		}
		if (url.endsWith("/"))
			url = url.substring(0, url.length() - 1);
		start = url.lastIndexOf('/');
		end = url.indexOf('?', start);
		if (end != -1)
			return url.substring(start + 1, end);
		return url.substring(start + 1);
	}

	private static String relativeToMobileUrl(String url) {
		url = url.replace("&amp;", "&");
		return "https://m.facebook.com" + url;
	}

	private static String getUrlParam(String url, String param) {
		String value = null;
		int startPos = url.indexOf(param + "=");
		if (startPos != -1) {
			startPos += param.length() + 1;
			int endPos = url.indexOf('&', startPos);
			if (endPos == -1)
				endPos = url.length();
			value = url.substring(startPos, endPos);
		}
		return value;
	}

	/**
	 * Attempt to decode external link wrapped by Facebook according to the following pattern:
	 * (http(s)://l(m).facebook.com)/l.php?u=<encoded URL>&h=<???>&enc=<???>&s=1
	 */
	private static String decodeUrl(String url) {
		if (url.contains("/l.php?")) {
			String encodedUrl = getUrlParam(url, "u");
			if (encodedUrl != null) {
				try {
					url = URLDecoder.decode(encodedUrl, "UTF-8");
				} catch (UnsupportedEncodingException ex) {
					logger.log(Level.SEVERE, "UTF-8 not supported", ex);
				}
			}
		}
		return url;
	}

	private static Person linkToPerson(Element linkElem) {
		if (linkElem.getAttribute("data-hovercard").contains("user.php"))	// link to an actual user profile?
			return new Person(getIdFromUrl(linkElem.getAttribute("href")), linkElem.getTextContent());
		return null;
	}

	private static Place linkToPlace(Element linkElem) {
		// can only check whether link target is a "page"; caller has to ensure that it refers to a place, and not a
		// person/group/...
		if (linkElem.getAttribute("data-hovercard").contains("page.php"))
			return new Place(getIdFromUrl(linkElem.getAttribute("href")), linkElem.getTextContent());
		return null;
	}

	private static Site linkToSite(Element linkElem) {
		// cannot reliably distinguish Site and Place, left to the caller
		if (linkElem.getAttribute("data-hovercard").contains("page.php"))
			return new Site(getIdFromUrl(linkElem.getAttribute("href")), linkElem.getTextContent());
		return null;
	}

	private static Date parseTimeStamp(Element timeElem) {
		Date time = null;
		String timeStr = timeElem.getAttribute("data-utime");
		try {
			time = new Date(Long.parseLong(timeStr) * 1000);
		} catch (NumberFormatException ex) {
			logger.warning("invalid timestamp '" + timeStr + "'");
		}
		return time;
	}

	private static void extractParticipantsAndPlace(NormalPostHeader header, List<Element> linkElem,
			Map<Collection<Person>, String> unresolvedPersons) {
		for (Element e : linkElem) {
			if (e.getAttribute("ajaxify").contains("/participants/?q=")) {
				// "and X more people"
				String origUrl = e.getAttribute("ajaxify");
				String url = "https://www.facebook.com/browse" +
						origUrl.substring(origUrl.indexOf("/participants/?q="));
				unresolvedPersons.put(header.getPersons(), url);
			} else {
				// link to user profile or page - assume the latter refers to a place
				Person person = linkToPerson(e);
				if (person != null) {
					header.getPersons().add(person);
				} else {
					Place place = linkToPlace(e);
					if (place != null)
						header.setPlace(place);
				}
			}
		}
	}

	private static class HeaderTypePattern {
		private final NormalPostHeader.Type type;
		private final String stringPattern;
		private final Pattern regexPattern;
		private final boolean isUnverified;

		public HeaderTypePattern(NormalPostHeader.Type type, String pattern, boolean isRegex, boolean isUnverified) {
			this.type = type;
			if (isRegex) {
				stringPattern = null;
				regexPattern = Pattern.compile(pattern, Pattern.DOTALL);	// header text may contain line breaks...
			} else {
				stringPattern = pattern;
				regexPattern = null;
			}
			this.isUnverified = isUnverified;
		}

		public NormalPostHeader.Type match(String text) {
			boolean isMatch = false;
			if (regexPattern != null)
				isMatch = regexPattern.matcher(text).find();
			else
				isMatch = text.contains(stringPattern);
			if (isMatch) {
				if (isUnverified)
					logger.warning("header '" + text + "' matches unverified pattern of post type " + type);
				return type;
			}
			return NormalPostHeader.Type.UNKNOWN;
		}
	}

	private static final List<HeaderTypePattern> headerTypePatterns;
	static {
		headerTypePatterns = new ArrayList<HeaderTypePattern>();
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.COVERPHOTOCHANGE,
				"Titelbild aktualisiert.", false, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.PROFILEPICCHANGE,
				"Profilbild geändert.", false, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.UPLOADEDVIDEO,
				"hat ein neues Video.* hochgeladen", true, false));	// sometimes the upload date is specified
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.LINKVIA,
				"via ", false, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.POSTLINK,
				"hat einen Link geteilt", false, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.LIKESLINK,
				"gefällt .+\\p{IsPunctuation}", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.RECOMMENDSLINK,
				"empfiehlt .+\\p{IsPunctuation}", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.POSTONTO,
				"hat .*(?:an)|(?:in) .+ Chronik (?:gepostet)|(?:geteilt)", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.POSTONTO,	// FB localization bug?
				"shared .+ to .+ timeline", true, true));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.SHAREDPHOTO,
				"hat .+ Foto geteilt", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.SHAREDALBUM,
				"hat .+ Album geteilt", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.SHAREDVIDEO,
				"hat .+ Video geteilt", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASONTHEROAD,
				"war mit .+ unterwegs", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASHEREWITH,
				"mit .+ hier:", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASINWITH,
				"war mit .+ in .+\\.", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASHERE,
				"war in .+\\.", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.ADDPHOTOSTOALBUM,
				"hat .+ Fotos? zu dem Album .+ hinzugefügt", true, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.UPLOADEDPHOTO,
				"hat .+ Fotos?.* hinzugefügt", true, false)); // sometimes a date is specified
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.FRIENDSWITH,
				"und .+ sind jetzt Freunde\\.", true, false));

		// lower priority to avoid conflict with more specific patterns for WASHEREWITH, WASONTHEROAD
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASHERE,
				"hier:", false, false));
		headerTypePatterns.add(new HeaderTypePattern(NormalPostHeader.Type.WASWITH,
				"mit .+", true, false));
	}

	private NormalPostHeader extractPostHeader(Element headerElem, String userName,
			Map<Collection<Person>, String> unresolvedPersons) {
		Element titleElem = findElement(headerElem, "./div/div/div/h5[@class=\"_5pbw\"] | " +
				"./div/div/div/h6[@class=\"_5pbw\"]");
		String headerText = titleElem.getTextContent();

		Element elem = findElement(titleElem, "./div[@class=\"fwn fcg\"]//span[starts-with(@class, \"fwb\")]/a[1]");
		if (elem == null) {
			// new variant since ~ February 2015
			elem = findElement(titleElem, "./div[@class=\"fwn fcg\"]/span[@class=\"fcg\"]/a[1]");
			if (elem == null) {
				logger.warning("post header '" + headerText + "' has unknown structure, ignoring");
				return null;	// give up and let caller deal with it
			}
		}
		Person sender = linkToPerson(elem);
		if (sender == null) {	// this happens when sender is a page
			logger.info("post header '" + headerText + "' without valid sender, ignoring");
			return null;
		}

		String postId = null;
		Date time = null;
		Place place = null;
		List<Element> subHeaderElements = findElements(headerElem,
				".//span[@class=\"fsm fwn fcg\"]/a[@class=\"_5pcq\"]");
		if (!subHeaderElements.isEmpty()) {
			// extract post ID; if the URL does not match the pattern given below, the header belongs to a "story" (e.g.
			// "answered n survey questions"), which has a different structure
			elem = subHeaderElements.get(0);
			String url = elem.getAttribute("href");
			if (url.contains("posts/"))
				postId = getIdFromUrl(url);

			elem = findElement(elem, "./abbr");
			time = parseTimeStamp(elem);

			if (subHeaderElements.size() > 1) {	// sub-header contains Place specification
				elem = subHeaderElements.get(1);
				place = new Place(getIdFromUrl(elem.getAttribute("href")), elem.getTextContent());
			}
		}

		// examine header text to determine post type
		NormalPostHeader.Type postType = NormalPostHeader.Type.NONE;
		headerText = headerText.replaceFirst(sender.getName(), "");	// remove sender name
		if (!headerText.isEmpty()) {
			for (HeaderTypePattern pattern : headerTypePatterns) {
				postType = pattern.match(headerText);
				if (postType != NormalPostHeader.Type.UNKNOWN)
					break;
			}
			if (postType == NormalPostHeader.Type.UNKNOWN) {
				if (headerText.endsWith(userName))
					postType = NormalPostHeader.Type.POSTONTO;
				else
					logger.warning("header could not be identified: '" + headerText + "'");
			}
		}

		// try to extract more information depending on type, e.g. shared link -> ID of original poster
		Set<Person> persons = new HashSet<Person>();
		Person sharedVia = null;
		boolean hasParticipantsOrPlace = false;
		List<Element> links = null;
		if (postType == NormalPostHeader.Type.LINKVIA)
			links = findElements(headerElem, ".//span[@class=\"fcg\"]/span[@class=\"fwb\"]/a");
		else
			links = findElements(headerElem, ".//span[@class=\"fcg\"]/a");

		switch (postType) {
		case LINKVIA:
			// Extract ID of original poster; no need to extract link, will be pulled out of container later.
			// If missing, link has been shared "via" a Facebook app instead of a person with a user profile.
			if (links.size() >= 2)
				sharedVia = linkToPerson(links.get(1));
			break;
		case SHAREDPHOTO:
		case SHAREDALBUM:
		case SHAREDVIDEO:
			if (links.size() >= 2)	// user can share own content, in which case there is no "via" link
				sharedVia = linkToPerson(links.get(0));
			break;
		case WASHERE:
		case WASHEREWITH:
		case WASINWITH:
		case WASWITH:
		case WASONTHEROAD:
			hasParticipantsOrPlace = true;
			break;
		case FRIENDSWITH:
			Person person = linkToPerson(links.get(0));
			if (person != null)
				persons.add(person);
			break;
		default:
			break;
		}

		NormalPostHeader header = new NormalPostHeader(postId, postType, sender, time, place, sharedVia, persons);
		if (hasParticipantsOrPlace)
			extractParticipantsAndPlace(header, links, unresolvedPersons);
		return header;
	}

	private String collectTextContent(Element container) {
		List<Element> content = findElements(container, ".//p");
		StringBuilder sb = new StringBuilder();
		for (Element e : content)
			sb.append(e.getTextContent());
		return sb.toString();
	}

	private Link extractLink(Element postContainer) {
		Element link = findElement(postContainer, ".//div[@class=\"_6m3\"]/div[@class=\"mbs _6m6\"]/a");
		if (link == null) {
			link = findElement(postContainer, ".//div[@class=\"_6m3\"]/a");
			if (link == null)
				return null;
		}
		return new Link(decodeUrl(link.getAttribute("href")));
	}

	private PhotoAlbum extractPhotoAlbum(Element postContainer) {
		Element headerElem = findElement(postContainer, "./div/div[@class=\"_3dp _29k\"]");
		Element link = findElement(headerElem, ".//span[@class=\"fcg\"]/a[contains(@href, \"/media/set/\")]");
		if (link == null)
			return null;

		String url = link.getAttribute("href");
		String id = getUrlParam(url, "set");
		if (id == null) {
			logger.warning("no photo album ID found in URL '" + url + "'");
			return null;
		}
		// If the ID starts with "ms.", this is a subset of an album, with the ID being a list of all included photos.
		// The ID can be arbitrarily long, and apparently does not contain any information about the original album, so
		// we ignore any albums of this type.
		if (id.startsWith("ms.")) {
			logger.info("ignoring synthetic photo album '" + id + "'");
			return null;
		}

		// in case of shared album, need to get name from share comment header
		String name;
		String shareComment = "";
		Element e = findElement(postContainer, ".//div[@class=\"mbs fwn fcg\"]/span/a");
		if (e != null) {
			name = e.getTextContent();
			e = findElement(postContainer, ".//div[@class=\"mtm _5pco\"]");
			if (e != null)
				shareComment = collectTextContent(e);
		} else
			name = link.getTextContent();

		return new PhotoAlbum(id, name, shareComment);
	}

	private EmbeddableObject extractPhotoOrVideo(Element postContainer) {
		// TODO: there is a third pattern, ".//div[@class=\"mtm\"]/div/div/a"
		Element link = findElement(postContainer, ".//div[@class=\"mtm\"]/div/a");
		if (link == null) {
			link = findElement(postContainer, ".//div[@class=\"mtm\"]/a");
			if (link == null)
				return null;
		}

		// quote from original post (optional)
		String shareComment = "";
		Element e = findElement(postContainer, ".//div[@class=\"mtm _5pco\"]");
		if (e != null)
			shareComment = collectTextContent(e);

		String url = link.getAttribute("href");
		if (url.contains("video.php")) {
			String id = getUrlParam(url, "v");
			if (id == null) {
				logger.warning("no video ID found in URL '" + url + "'");
				return null;
			}

			String title = "";
			e = findElement(link, "./div/div[1]");
			if (e != null)
				title = e.getTextContent();

			return new Video(id, title, shareComment);
		} else {
			String id = getUrlParam(url, "fbid");
			String albumId;
			if (id != null) {	// original content
				albumId = getUrlParam(url, "set");
				if (albumId == null) {
					logger.warning("no photo album ID found in URL '" + url + "'");
					return null;
				}
			} else {	// shared photo?
				// URL pattern is "/<userId>/photos/<albumId>/<photoId>/?type=1"
				int startPos = url.indexOf("photos/");
				if (startPos != -1) {
					startPos += 7;
					int endPos = url.indexOf('/', startPos);
					albumId = url.substring(startPos, endPos);
					startPos = endPos + 1;
					endPos = url.indexOf('/', startPos);
					id = url.substring(startPos, endPos);
				} else	// other kind of content, not handled yet
					return null;
			}

			if (albumId.startsWith("ms.")) {
				logger.info("ignoring photo in synthetic album '" + albumId + "'");
				return null;
			}
			return new Photo(id, albumId, shareComment);
		}
	}

	private EmbeddableNormalPost extractSharedPost(NormalPostHeader header, Element postContainer) {
		Element link = findElement(postContainer, ".//div[@class=\"_5pcn fwb\"]/a");
		if (link == null)
			return null;

		// URL pattern: "/<user ID>/posts/<post ID>?fref=nf"
		String url = link.getAttribute("href");
		int startPos = url.indexOf("/posts/");
		if (startPos == -1)
			return null;
		String personId = url.substring(url.indexOf('/') + 1, startPos);
		int endPos = url.lastIndexOf('?');
		if (endPos == -1)
			endPos = url.length();
		String postId = url.substring(startPos + 7, endPos);
		Person originalAuthor = new Person(personId, link.getTextContent());

		String content = "";
		Element e = findElement(postContainer, ".//div[@class=\"mtm _5pco\"]");
		if (e != null)
			content = collectTextContent(e);

		// for all other media types, the original poster can be extracted from the post header
		header.setSharedVia(originalAuthor);

		return new EmbeddableNormalPost(new NormalPostHeader(postId, originalAuthor), content, null, null, null);
	}

	private EmbeddableObject extractEmbeddedContent(NormalPostHeader header, Element postContainer) {
		EmbeddableObject obj = extractLink(postContainer);
		if (obj != null)
			return obj;
		obj = extractPhotoAlbum(postContainer);
		if (obj != null)
			return obj;
		obj = extractPhotoOrVideo(postContainer);
		if (obj != null)
			return obj;
		return extractSharedPost(header, postContainer);
	}

	private NormalPost extractNormalPost(NormalPostHeader header, Element postContainer,
			Map<Collection<Person>, String> unresolvedPersons) {
		String content = "";
		Set<Person> mentionedPersons = new HashSet<Person>();
		Set<Site> mentionedSites = new HashSet<Site>();
		Element contentElem = findElement(postContainer, "./div[@class=\"_5pbx userContent\"]");
		if (contentElem != null) {
			content = collectTextContent(contentElem);

			// extract mentions
			for (Element link : findElements(contentElem, ".//a")) {
				Person mentionedUser = linkToPerson(link);
				if (mentionedUser != null) {
					mentionedPersons.add(mentionedUser);
				} else {
					Site mentionedSite = linkToSite(link);
					if (mentionedSite != null)
						mentionedSites.add(mentionedSite);
				}
			}

			// secondary content: kind of a second header, may contain additional people/place information
			List<Element> secondaryElem = findElements(contentElem, "./span[@class=\"userContentSecondary _c24\"]/a");
			extractParticipantsAndPlace(header, secondaryElem, unresolvedPersons);
		}

		EmbeddableObject mediaContent = extractEmbeddedContent(header, postContainer);

		// Some NormalPosts have a post ID and some don't, for example profile picture / cover photo change messages and
		// posted photos.
		if (header.getId() != null)
			return new EmbeddableNormalPost(header, content, mediaContent, mentionedPersons, mentionedSites);
		return new NormalPost(header, content, mediaContent, mentionedPersons, mentionedSites);
	}

	private SmallPost extractSmallPost(NormalPostHeader header, Element postContainer) {
		String content = "";
		Element contentElem = findElement(postContainer, "./div[@class=\"_5pbx userContent\"]");
		if (contentElem != null)
			content = collectTextContent(contentElem);
		return new SmallPost(new SmallPostHeader(header.getSender(), header.getTime()), content);
	}

	private LifeEvent extractLifeEvent(NormalPostHeader header, Element postContainer) {
		String text = "";
		String title = "";
		String subTitle = "";

		Element contentElem = findElement(postContainer, ".//div[@class=\"mtm\"]/div");
		if (contentElem != null) {
			Element e = findElement(contentElem, "./div[@class=\"_6nm\"]");
			if (e != null)
				text = e.getTextContent();

			List<Element> items = findElements(contentElem, "./div[starts-with(@class, \"mvl\")]/div");
			if (!items.isEmpty()) {
				title = items.get(0).getTextContent();
				if (items.size() > 1)
					subTitle = items.get(1).getTextContent();
			}
		} else {	// small life event without content area, get title from header
			Element titleElem = findElement(postContainer, "./div/div[@class=\"_3dp _29k\"]/div/div/div/" +
					"h5[@class=\"_5pbw\"]");
			title = titleElem.getTextContent();
		}

		// TODO: life events can optionally have participants and a place
		return new LifeEvent(title, header.getTime(), subTitle, text);
	}

	private Comment extractComment(Element commentElem, Map<Comment, String> unresolvedLikes) {
		Element e = findElement(commentElem, ".//a[ends-with(@class, \"UFICommentActorName\")]");
		if (e == null) {
			// If the account of the comment's author no longer exists, the author's name is wrapped in a "span" instead
			// of an "a" element.
			logger.warning("comment without sender ID, ignoring");
			return null;
		}
		Person sender = new Person(getIdFromUrl(e.getAttribute("href")), e.getTextContent());

		e = findElement(commentElem, ".//span[@class=\"UFICommentBody\"]");
		String text = e.getTextContent();

		Date time = null;
		e = findElement(commentElem, ".//abbr");
		if (e != null) {
			time = parseTimeStamp(e);
		} else {
			// Seen while processing one particular user profile: HTML source contains an "abbr" element in the expected
			// place, but htmlcleaner appears to drop it for unknown reasons.
			logger.warning("comment without timestamp");
		}

		Comment c = new Comment(sender, time, text);

		String likeLink = null;
		e = findElement(commentElem, ".//a[@class=\"UFICommentLikeButton\"]");
		if (e != null) {
			likeLink = e.getAttribute("href");
			unresolvedLikes.put(c, relativeToMobileUrl(likeLink));
		}

		return c;
	}

	private Post extractPost(Element postContainer, String userName, Map<Post, String> unresolvedPostLikes,
			Map<Comment, String> unresolvedCommentLikes, Map<Collection<Person>, String> unresolvedPersons) {
		Element headerElem = findElement(postContainer, "./div/div[@class=\"_3dp _29k\"]");
		// some timeline elements "wrap" the actual element; in this case, the header is nested deeper
		if (headerElem == null) {
			logger.warning("header not found, skipping post");
			return null;
		}
		NormalPostHeader header = extractPostHeader(headerElem, userName, unresolvedPersons);
		if (header == null)
			return null;

		Post post;
		if (postContainer.getAttribute("class").equals("userContentWrapper _5pcr _3ccb")) {
			Element e = (Element) postContainer.getFirstChild();
			if (e.getAttribute("class").equals("_4r_y"))
				post = extractNormalPost(header, postContainer, unresolvedPersons);
			else
				post = extractLifeEvent(header, postContainer);
		} else
			post = extractSmallPost(header, postContainer);

		// extract likes
		for (Element like : findElements(postContainer, ".//div[@class=\"UFILikeSentenceText\"]/span/a")) {
			String url = like.getAttribute("href");
			if (url.contains("/browse/likes?id="))
				unresolvedPostLikes.put(post, relativeToMobileUrl(url));
			else	// has different kind of "data-hovercard", so cannot use linkToPerson
				post.getLikes().add(new Person(getIdFromUrl(like.getAttribute("href")), like.getTextContent()));
		}

		// extract comments
		List<Element> comments = findElements(postContainer, ".//div[@class=\"UFICommentContentBlock\"]");
		for (Element commentElem : comments) {
			Comment c = extractComment(commentElem, unresolvedCommentLikes);
			if (c != null)
				post.getComments().add(c);
		}

		return post;
	}

	private Set<Person> extractLikes(String url) {
		Set<Person> persons = new HashSet<Person>();
		Document doc = fetchPage(url);
		while (true) {
			for (Element e : findElements(doc, "//div[@id=\"root\"]/div/div/div/div/div/a[1]"))
				persons.add(new Person(getIdFromUrl(e.getAttribute("href")), e.getTextContent()));

			// fetch next page
			Element e = findElement(doc, "//div[@id=\"m_more_item\"]/a");
			if (e == null)
				break;
			doc = fetchPage(relativeToMobileUrl(e.getAttribute("href")));
		}
		return persons;
	}

	private Set<Person> extractPersonList(String url) {
		Set<Person> persons = new HashSet<Person>();
		Document doc = fetchPage(url);
		for (Element e : findElements(doc, "//div[@class=\"_6a _6b\"]/div[@class=\"fsl fwb fcb\"]/a"))
			persons.add(new Person(getIdFromUrl(e.getAttribute("href")), e.getTextContent()));
		return persons;
	}

	/**
	 * Extracts the number of friends shown on the profile's friends list. If this is not found, return 0.
	 * @param doc DOM of "http://m.facebook.com/"+userId+"?v=friends&refid=17"
	 */
	private int extractNumberOfFriends(Document doc) {
		int numFriends = 0;
		Element e = findElement(doc, "//div[@id=\"root\"]/div/h3");
		if (e != null) {
			String text = e.getTextContent();
			if (text.startsWith("Freunde (")) {
				String numStr = text.substring(9, text.lastIndexOf(')'));
				numStr = numStr.replace(".", "");	// remove thousands separators
				try {
					numFriends = Integer.parseInt(numStr);
				} catch (NumberFormatException ex) {
					logger.warning("invalid number of friends '" + numStr + "' (context='" + text + "')");
				}
			}
		}
		return numFriends;
	}

	/**
	 * @param doc DOM of "http://m.facebook.com/"+userId+"?v=friends&refid=17"
	 */
	private Set<Person> extractFriends(Document doc) {
		// number of extracted friends often does not match up with number of friends reported by Facebook;
		//	suspect an off-by-one error in Facebook's pagination code
		Set<Person> friends = new HashSet<Person>();
		while (true) {
			for (Element e : findElements(doc, "//div[@id=\"root\"]/div/div/div/div/div/a[1]"))
				friends.add(new Person(getIdFromUrl(e.getAttribute("href")), e.getTextContent()));

			// fetch next page
			Element e = findElement(doc, "//div[@id=\"m_more_friends\"]/a");
			if (e == null)
				break;
			doc = fetchPage(relativeToMobileUrl(e.getAttribute("href")));
		}
		return friends;
	}

	private void extractSiteLikesSinglePage(Document doc, boolean isAppSection, Set<Site> likedSites,
			List<String> urls) {
		// extract likes
		String path;
		if (isAppSection)
			path = "//div[@id=\"timelineBody\"]/div/div/div/div/div/div/div/a[1]";
		else
			path = "//div[@id=\"root\"]/div/div/div/div/div/a[1]";
		for (Element e : findElements(doc, path))
			likedSites.add(new Site(getIdFromUrl(e.getAttribute("href")), e.getTextContent()));

		// find "show more" URLs
		for (Element e : findElements(doc, "//div[@id=\"m_more_item\"]/a"))
			urls.add(relativeToMobileUrl(e.getAttribute("href")));
	}

	private Set<Site> extractSiteLikes() {
		Set<Site> likedSites = new HashSet<Site>();
		Document doc = fetchPageByName("likes");

		LinkedList<String> urls = new LinkedList<String>();
		extractSiteLikesSinglePage(doc, false, likedSites, urls);

		// find additional "show more" URLs on first page
		for (Element e : findElements(doc, "//a[contains(@href, \"/timeline/app_section/\")]"))
			urls.add(relativeToMobileUrl(e.getAttribute("href")));

		while (!urls.isEmpty()) {
			String url = urls.poll();
			doc = fetchPage(url);
			extractSiteLikesSinglePage(doc, url.contains("/timeline/app_section/"), likedSites, urls);
		}
		return likedSites;
	}

	/** Extract people mentioned in education or work history. */
	private Set<Person> extractSecondaryContentExperience(Element secondary,
			Map<Collection<Person>, String> unresolvedPersons) {
		Set<Person> persons = new HashSet<Person>();
		List<Element> secondaryLinks = findElements(secondary, "./a");
		if (secondaryLinks.size() > 0) {
			Element person = secondaryLinks.get(0);
			persons.add(new Person(getIdFromUrl(person.getAttribute("href")), person.getTextContent()));
		}
		if (secondaryLinks.size() == 2) {
			Element person = secondaryLinks.get(1);
			if (person.getAttribute("href").contains("/browse/experiencetags?id=")) {
				// "with X and Y other people"
				String moreLink = person.getAttribute("href");
				if (moreLink != null)
					unresolvedPersons.put(persons, relativeToMobileUrl(moreLink));
			} else {
				// "with X and Y"
				persons.add(new Person(getIdFromUrl(person.getAttribute("href")), person.getTextContent()));
			}
		}
		return persons;
	}

	private Map<String, Element> parseDetailsSection(Document doc, String name) {
		Map<String, Element> entries = new HashMap<String, Element>();
		List<Element> rows = findElements(doc, "//div[@id=\"" + name + "\"]/div/div/div");
		for (Element row : rows) {
			String key = row.getAttribute("title");
			Element value = findElement(row, "./table/tbody/tr/td[2]/div");	// class ID varies for different users!
			entries.put(key, value);
		}
		return entries;
	}

	private String[] parseOptionalLink(Element e) {
		String name;
		String id = "";
		Element link = findElement(e, "./a");
		if (link != null) {
			name = link.getTextContent();
			id = getIdFromUrl(link.getAttribute("href"));
		} else
			name = e.getTextContent();
		return new String[] { name, id };
	}

	private static boolean isTimePeriod(String text) {
		return (text.contains(" - ") || text.contains(" bis ") ||
				text.startsWith("Ab ") || text.startsWith("Abschlussjahrgang "));
	}

	private PersonDetails extractPersonDetails() {
		Document doc = fetchPageByName("info");

		// common attributes
		char gender = 'u';	// 'u' for unknown
		char interestedIn = 'u';
		Date birthday = null;
		Set<String> spokenLanguages = new HashSet<String>();
		String[] religion = new String[] { "", "" };
		String[] politics = new String[] { "", "" };
		Map<String, Element> rows = parseDetailsSection(doc, "basic-info");
		for (Map.Entry<String, Element> row : rows.entrySet()) {
			if (row.getKey().equals("Geschlecht")) {
				char first = Character.toLowerCase(row.getValue().getTextContent().charAt(0));
				if (first == 'm')	// depends on UI language
					gender = 'm';
				else if (first == 'w')
					gender = 'f';
			} else if (row.getKey().equals("Interessiert an")) {
				String preference = row.getValue().getTextContent();
				if (preference.equals("Männer"))
					interestedIn = 'm';
				else if (preference.equals("Frauen"))
					interestedIn = 'f';
				else if (preference.equals("Männer und Frauen"))
					interestedIn = 'b';
			} else if (row.getKey().equals("Geburtstag")) {
				String birth = row.getValue().getTextContent();
				SimpleDateFormat format = new SimpleDateFormat("d. MMMM y", Locale.GERMANY);
				birthday = format.parse(birth, new ParsePosition(0));
				if (birthday == null) {
					// parse error, most likely due to missing year: use 1604 (leap year)
					birthday = format.parse(birth + " 1604", new ParsePosition(0));
				}
			} else if (row.getKey().equals("Sprachen")) {
				String languages = row.getValue().getTextContent().replace(" und", ",");
				spokenLanguages.addAll(Arrays.asList(languages.split(", ")));
			} else if (row.getKey().equals("Religiöse Ansichten")) {
				religion = parseOptionalLink(row.getValue());
			} else if (row.getKey().equals("Politische Einstellung")) {
				politics = parseOptionalLink(row.getValue());
			}	// ignore all others, unknown structure
		}

		String telephoneNumber = "";
		String address = "";
		String homepage = "";
		String email = "";
		rows = parseDetailsSection(doc, "contact-info");
		for (Map.Entry<String, Element> row : rows.entrySet()) {
			if (row.getKey().equals("Handy")) { // others exist, e.g. "Privat" or "Arbeit"!
				telephoneNumber = row.getValue().getTextContent();
			} else if (row.getKey().equals("Anschrift")) {
				address = row.getValue().getTextContent();
			} else if (row.getKey().equals("Webseite")) {
				// can occur multiple times, but currently we only keep one URL
				Element link = (Element) row.getValue().getFirstChild();
				homepage = decodeUrl(link.getAttribute("href"));
			} else if (row.getKey().equals("E-Mail-Adresse")) {
				email = row.getValue().getTextContent();
			}
		}

		String[] currentResidence = new String[] { "", "" };
		String[] hometown = new String[] { "", "" };
		rows = parseDetailsSection(doc, "living");
		for (Map.Entry<String, Element> row : rows.entrySet()) {
			if (row.getKey().equals("Aktueller Wohnort")) {
				currentResidence = parseOptionalLink(row.getValue());
			} else if (row.getKey().equals("Heimatstadt")) {
				hometown = parseOptionalLink(row.getValue());
			}
		}

		// biography, i.e. "about <first name>" section
		String bio = "";
		Element textElement = findElement(doc, "//div[@id=\"bio\"]/div/div/div");
		if (textElement != null)
			bio = textElement.getTextContent();

		// favorite quotes
		String quotes = "";
		textElement = findElement(doc, "//div[@id=\"quote\"]/div/div/div");
		if (textElement != null)
			quotes = textElement.getTextContent();

		Set<FamilyMember> family = new HashSet<FamilyMember>();
		List<Element> items = findElements(doc, "//div[@id=\"family\"]/div/div/div");
		for (Element item : items) {
			// If the other person has not yet accepted the request, some elements are missing. Also, we're deliberately
			// ignoring relatives without a Facebook account (first "h3" does not contain an "a"), because they lack an
			// ID and thus cannot be represented by a Person object.
			Element link = findElement(item, "./div/h3[1]/a");
			Element desc = findElement(item, "./div/h3[2]");
			if ((link != null) && (desc != null)) {
				Person relative = new Person(getIdFromUrl(link.getAttribute("href")), link.getTextContent());
				family.add(new FamilyMember(relative, desc.getTextContent()));
			}
		}

		String relationshipStatus = "";
		Element relItem = findElement(doc, "//div[@id=\"relationship\"]/div/div/div/div");
		if (relItem != null) {
			relationshipStatus = relItem.getTextContent();
			int cutPos = -1;
			Element e = findElement(relItem, "./a");
			if (e != null) {
				Person partner = new Person(getIdFromUrl(e.getAttribute("href")), e.getTextContent());
				family.add(new FamilyMember(partner, "Partner"));
				cutPos = relationshipStatus.indexOf(" mit");
			} else	// partner not specified / does not have a Facebook account
				cutPos = relationshipStatus.indexOf(" seit");
			if (cutPos != -1)
				relationshipStatus = relationshipStatus.substring(0, cutPos);
		}

		Map<Collection<Person>, String> unresolvedPersons = new HashMap<Collection<Person>, String>();

		List<WorkItem> work = new ArrayList<WorkItem>();
		items = findElements(doc, "//div[@id=\"work\"]/div/div/div");
		for (Element item : items) {
			List<Element> lines = findElements(item, "./div/div/div/span");
			String[] workName = parseOptionalLink(lines.get(0));

			String title = "";
			String timePeriod = "";
			String place = "";
			String description = "";
			for (int i = 1; i < lines.size(); i++) {
				Element line = lines.get(i);
				Element e = findElement(line, "./span");
				if (e != null) {
					description = e.getTextContent();
				} else {
					String text = line.getTextContent();
					if (timePeriod.isEmpty() && isTimePeriod(text))
						timePeriod = text;
					else if (title.isEmpty())
						title = text;
					else if (place.isEmpty())
						place = text;
				}
			}

			List<WorkProject> projects = null;
			List<Element> elements = findElements(item, "./div/div/ul/li");
			if (!elements.isEmpty()) {
				projects = new ArrayList<WorkProject>();
				for (Element container : elements) {
					String projectName = "";
					String projectTimePeriod = "";
					String projectDescription = "";
					Set<Person> secondary = Collections.emptySet();
					lines = findElements(container, "./div/span");
					for (Element line : lines) {
						Element e = findElement(line, "./strong");
						if (e != null) {
							projectName = e.getTextContent();
						} else {
							e = findElement(line, "./span");
							if (e != null) {
								secondary = extractSecondaryContentExperience(e, unresolvedPersons);
							} else {
								String text = line.getTextContent();
								if (projectTimePeriod.isEmpty() && isTimePeriod(text))
									projectTimePeriod = text;
								else
									projectDescription = text;
							}
						}
					}
					projects.add(new WorkProject(projectName, projectTimePeriod, projectDescription, secondary));
				}
			}

			work.add(new WorkItem(workName[0], workName[1], title, timePeriod, place, description, projects));
		}

		List<EducationItem> education = new ArrayList<EducationItem>();
		items = findElements(doc, "//div[@id=\"education\"]/div/div/div");
		for (Element item : items) {
			List<Element> lines = findElements(item, "./div/div/div/span");
			String[] educationName = parseOptionalLink(lines.get(0));

			String timePeriod = "";
			String type = "";
			String field = "";
			for (int i = 1; i < lines.size(); i++) {
				Element line = (Element) lines.get(i);
				Element e = findElement(line, "./span");
				if (e != null) {
					field = e.getTextContent();
				} else {
					String text = line.getTextContent();
					if (timePeriod.isEmpty() && isTimePeriod(text))
						timePeriod = text;
					else
						type = text;
				}
			}

			List<EducationClass> classes = null;
			List<Element> elements = findElements(item, "./div/div/ul/li");
			if (!elements.isEmpty()) {
				classes = new ArrayList<EducationClass>();
				for (Element container : elements) {
					String className = "";
					String classDescription = "";
					Set<Person> secondary = Collections.emptySet();
					lines = findElements(container, "./div/span");
					for (Element line : lines) {
						Element e = findElement(line, "./strong");
						if (e != null) {
							className = e.getTextContent();
						} else {
							e = findElement(line, "./span");
							if (e != null)
								secondary = extractSecondaryContentExperience(e, unresolvedPersons);
							else
								classDescription = line.getTextContent();
						}
					}
					classes.add(new EducationClass(className, classDescription, secondary));
				}
			}

			education.add(new EducationItem(educationName[0], educationName[1], timePeriod, type, field, classes));
		}

		// fill in Secondary (resolve person links)
		for (Map.Entry<Collection<Person>, String> e : unresolvedPersons.entrySet())
			e.getKey().addAll(extractLikes(e.getValue()));

		return new PersonDetails(bio, quotes, gender, interestedIn, birthday, spokenLanguages, religion[0], religion[1],
				politics[0], politics[1], telephoneNumber, address, homepage, email, relationshipStatus,
				currentResidence[0], currentResidence[1], hometown[0], hometown[1], family, education, work);
	}

	private static String detectLanguage(Collection<Post> posts) {
		StringBuilder text = new StringBuilder();
		for (Post post : posts) {
			String postText = post.getText();
			if (postText != null)
				text.append(postText);
			if (post.getComments() != null)
				for (Comment comm : post.getComments())
					text.append(comm.getText());
		}

		String language = "";
		try {
			if (text.length() > 0) {
				Detector languageDetector = DetectorFactory.create();
				languageDetector.append(text.toString());
				language = languageDetector.detect();
				logger.fine("detected language: " + language);
			} else
				logger.info("no textual content, skipping language detection");
		} catch (LangDetectException e) {
			logger.log(Level.WARNING, "error detecting languge", e);
		}
		return language;
	}

	@Override
	public UserProfile extractProfile() {
		Document doc = fetchPage("https://www.facebook.com/" + userId);

		Element nameElement = findElement(doc, "//div[@id=\"fbProfileCover\"]//a[@class=\"_8_2\"]");
		String userName = nameElement.getTextContent();
		int pos = userName.indexOf('(');
		if (pos >= 0)	// remove "alternate name" in parentheses if present
			userName = userName.substring(0, pos).trim();

		List<Post> posts = new ArrayList<Post>();
		Set<EmbeddableNormalPost> seenEmbeddablePosts = new HashSet<EmbeddableNormalPost>();

		Map<Post, String> unresolvedPostLikes = new HashMap<Post, String>();
		Map<Comment, String> unresolvedCommentLikes = new HashMap<Comment, String>();
		Map<Collection<Person>, String> unresolvedPersons = new HashMap<Collection<Person>, String>();

		logger.fine("Extracting posts...");
		List<Element> postElements = findElements(doc, "//div[starts-with(@class, \"userContentWrapper\")]");
		for (Element postElement : postElements) {
			Post post = extractPost(postElement, userName, unresolvedPostLikes, unresolvedCommentLikes,
					unresolvedPersons);
			if (post != null) {
				if (logger.isLoggable(Level.FINEST))
					logger.finest(post.getClass().getSimpleName() + ": " + JSON.encode(post));

				// Under rare and unknown circumstances, a profile contains the exact same post twice. By design,
				// FacebookDao cannot handle duplicate EmbeddableNormalPosts, so they have to be filtered out here.
				if (post instanceof EmbeddableNormalPost) {
					EmbeddableNormalPost embeddablePost = (EmbeddableNormalPost) post;
					if (seenEmbeddablePosts.add(embeddablePost))
						posts.add(post);
					else
						logger.info("duplicate post, ID = " + embeddablePost.getId());
				} else
					posts.add(post);
			}
		}

		logger.fine("Checking likelinks...");
		for(Map.Entry<Post, String> e : unresolvedPostLikes.entrySet()) {
			String url = e.getValue();
			logger.finer(url);
			e.getKey().getLikes().addAll(extractLikes(url));
		}
		for(Map.Entry<Comment, String> e : unresolvedCommentLikes.entrySet()) {
			String url = e.getValue();
			logger.finer(url);
			e.getKey().getLikes().addAll(extractLikes(url));
		}

		logger.fine("Extracting full person lists...");
		for(Map.Entry<Collection<Person>, String> e : unresolvedPersons.entrySet()) {
			String url = e.getValue();
			logger.finer(url);
			e.getKey().addAll(extractPersonList(url));
		}

		logger.fine("Extracting friends/likes/details...");
		doc = fetchPageByName("friends");
		int numFriends = extractNumberOfFriends(doc);
		Set<Person> friends = extractFriends(doc);

		Set<Site> likedSites = extractSiteLikes();
		PersonDetails details = extractPersonDetails();
		if (logger.isLoggable(Level.FINEST))
			logger.finest("PersonDetails: " + JSON.encode(details));

		logger.fine("Detecting language...");
		String language = detectLanguage(posts);

		logger.info("Finished scraping profile.");
		return new UserProfile(new Person(userId, userName), details, posts, numFriends, friends, likedSites, language);
	}

}
