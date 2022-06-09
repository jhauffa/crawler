package edu.tum.cs.crawling.facebook.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.ElementNotVisibleException;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.remote.UnreachableBrowserException;
import org.openqa.selenium.support.ui.WebDriverWait;

import com.google.common.base.Predicate;

public class FirefoxCrawler {

	private static final Logger logger = Logger.getLogger(FirefoxCrawler.class.getName());
	private static final int defaultTimeoutSeconds = 10 * 60;

	private final WebDriver driver;
	private final JavascriptExecutor js;

	private class NewContentPredicate implements Predicate<WebDriver> {
		public final long curHeight;
		public long newHeight;

		public NewContentPredicate(long curHeight) {
			this.curHeight = curHeight;
		}

		@Override
		public boolean apply(WebDriver driver) {
			newHeight = (Long) js.executeScript("if (document.readyState != \"complete\") { return -1; } " +
					"return document.body.offsetHeight;");
			return (newHeight > curHeight);
		}
	}

	private long waitForMoreContent(long curHeight, int timeout) {
		WebDriverWait wait = new WebDriverWait(driver, timeout /* s */, 100 /* ms */);
		NewContentPredicate hasNewContent = new NewContentPredicate(curHeight);
		try {
			wait.until(hasNewContent);
		} catch (TimeoutException ex) {
			logger.fine("no new content");
			return curHeight;
		}
		return hasNewContent.newHeight;
	}

	/** Scrolls down the current web page until no more content is loaded. */
	private void scrollDown(int timeout) {
		final int maxScrollDowns = 150;	// prevent an infinite loop
		int i = 0;
		long curPos;
		long targetPos = (Long) js.executeScript("return document.body.offsetHeight");
		do {
			logger.finer("scrolling...");
			js.executeScript("window.scrollTo(0," + targetPos + ");");
			curPos = targetPos;
			targetPos = waitForMoreContent(curPos, timeout);
		} while ((++i <= maxScrollDowns) && (curPos < targetPos));
	}

	private void waitForPageComplete() {
		WebDriverWait wait = new WebDriverWait(driver, defaultTimeoutSeconds);
		Predicate<WebDriver> isLoaded = new Predicate<WebDriver>() {
			@Override public boolean apply(WebDriver driver) {
				return js.executeScript("return document.readyState").equals("complete");
			}
		};
		wait.until(isLoaded);
	}

	private void fetchPage(String url, boolean scrollDown, Map<String, String> pageSource) {
		driver.get(url);
		if (scrollDown)
			scrollDown(1);
		else
			waitForPageComplete();
		pageSource.put(url, driver.getPageSource());
	}

	private void fetchPageByUserId(String userId, String page, Map<String, String> pageSource) {
		String url;
		if (userId.matches("[0-9]+"))	// numeric user ID
			url = "https://m.facebook.com/profile.php?v=" + page + "&id=" + userId + "&refid=17";
		else
			url = "https://m.facebook.com/" + userId + "?v=" + page + "&refid=17";
		fetchPage(url, false, pageSource);
	}

	private void fetchMorePages(String buttonId, Map<String, String> pageSource) {
		while (true) {
			List<WebElement> elements = driver.findElements(By.xpath("//div[@id=\"" + buttonId + "\"]/a"));
			if (elements.isEmpty())
				break;

			String url = elements.get(0).getAttribute("href");
			fetchPage(url, false, pageSource);
		}
	}

	private void handleTimelineElement(WebElement frame, String userId, Set<String> unresolvedParticipants,
			Set<String> unresolvedLikes) {
		// extract links to lists of participants from header or "secondary content" (optional, regular posts only)
		for (WebElement e : frame.findElements(By.xpath("./div/span[starts-with(@class, \"userContentSecondary\")]" +
				"//a[contains(@ajaxify, \"/participants/?q=\")] | ./div/div[@class=\"_3dp _29k\"]" +
				"//a[contains(@ajaxify, \"/participants/?q=\")]"))) {
			String origUrl = e.getAttribute("ajaxify");
			String url = "https://www.facebook.com/browse" + origUrl.substring(origUrl.indexOf("/participants/?q="));
			unresolvedParticipants.add(url);
		}

		// matches both post likes and comment likes
		for (WebElement e : frame.findElements(By.xpath(".//a[contains(@href, \"/browse/likes?id=\")]")))
			unresolvedLikes.add(e.getAttribute("href").replaceFirst("www\\.", "m."));	// convert to mobile link
	}

	private void handlePersonDetails(Set<String> unresolvedCoworkers) {
		for (WebElement e : driver.findElements(By.xpath("//div[@id=\"work\" or @id=\"education\"]" +
				"//a[contains(@href, \"/browse/experiencetags?id=\")]")))
			unresolvedCoworkers.add(e.getAttribute("href"));
	}

	private void handleSiteLikes(Map<String, String> pageSource) {
		final String moreLinkPath = "//div[@id=\"m_more_item\"]/a";
		LinkedList<String> urls = new LinkedList<String>();

		// find "show more" URLs on current page
		List<WebElement> elements = driver.findElements(By.xpath("//a[contains(@href, \"/timeline/app_section/\")]"));
		for (WebElement e : elements)
			urls.add(e.getAttribute("href"));
		elements = driver.findElements(By.xpath(moreLinkPath));
		for (WebElement e : elements)
			urls.add(e.getAttribute("href"));

		while (!urls.isEmpty()) {
			fetchPage(urls.poll(), false, pageSource);
			elements = driver.findElements(By.xpath(moreLinkPath));
			for (WebElement e : elements)
				urls.add(e.getAttribute("href"));
		}
	}

	private static void clickIfLocalTarget(WebElement e) {
		String targetUrl = e.getAttribute("href");
		if (targetUrl.endsWith("#"))
			e.click();
		else
			logger.fine("ignoring element with target URL '" + targetUrl + "'");
	}

	private void expandPosts(String buttonPath, boolean allowNonLocalUrls) {
		final int maxClicks = 150;
		int count = 0;
		List<WebElement> elements;
		do {
			elements = driver.findElements(By.xpath(buttonPath));
			for (WebElement e : elements) {
				try {
					if (allowNonLocalUrls)
						e.click();
					else
						clickIfLocalTarget(e);
				} catch (StaleElementReferenceException ex) {
					// happens sometimes when expanding comments, ignore
				} catch (ElementNotVisibleException ex) {
					// happens sometimes when expanding small posts, ignore
				}
				if (++count > maxClicks) {
					logger.fine("too many further posts");
					return;
				}
			}
		} while (!elements.isEmpty());
	}

	private boolean clickIfPresent(String path) {
		List<WebElement> elements = driver.findElements(By.xpath(path));
		for (WebElement e : elements) {
			try {
				clickIfLocalTarget(e);
			} catch (StaleElementReferenceException ex) {
				// happens sometimes when expanding longer posts, ignore
			} catch (ElementNotVisibleException ex) {
				// happens sometimes when expanding longer comments, ignore
			}
		}
		return !elements.isEmpty();
	}

	private void scrapeCurrentProfile(String userId, String userName, Map<String, String> pageSource) {
		logger.fine("scrolling down");
		scrollDown(5);

		logger.fine("clicking \"show older messages\"");
		List<WebElement> elements = driver.findElements(By.xpath("//a[@class=\"uiMorePagerPrimary\"]"));
		if (!elements.isEmpty()) {
			try {
				clickIfLocalTarget(elements.get(0));
			} catch (ElementNotVisibleException ex) {
				logger.warning("button not visible");
			}

			logger.fine("scrolling down again");
			scrollDown(5);
		}

		logger.fine("showing all messages for each year");
		// change post selection from "highlights" to "all messages" for each year; when starting at the top, it
		// sometimes happens that older messages are hidden, so start with the earliest year and move upwards
		elements = driver.findElements(By.xpath("//div[@class=\"_6a uiPopover uiHeaderActions\"]/a"));
		for (int i = elements.size() - 1; i >= 0; i--) {
			WebElement e = elements.get(i);
			try {
				e.click();	// open the menu

				List<WebElement> mi = driver.findElements(
						By.xpath("//li[@class=\"_54ni __MenuItem\"]/a/span/span/span"));
				WebElement menuItem = mi.get(mi.size() - 1);	// each menu creates its own menu item
				menuItem.click();
			} catch (StaleElementReferenceException ex) {
				continue;	// happens very rarely, possibly some kind of race with the JS code
			} catch (ElementNotVisibleException ex) {
				continue;
			}

			scrollDown((i == 0) ? 5 : 2);
		}

		// TODO: this happens frequently, but clicking the button may collapse other sections; needs investigation
		elements = driver.findElements(By.xpath("//a[@class=\"pam uiBoxLightblue uiMorePagerPrimary\"]"));
		if (!elements.isEmpty())
			logger.info("found button 'uiMorePagerPrimary', some posts might not be crawled");

		logger.fine("expanding more SmallPosts");
		expandPosts("//a[@class=\"_44b2\"]", true);

		logger.fine("expanding comments of SmallPosts");
		clickIfPresent("//a[@class=\"UFIBlingBox uiBlingBox feedbackBling\" and contains(@aria-label, \"Kommentar\")]");

		logger.fine("expanding longer posts");
		clickIfPresent("//a[@class=\"see_more_link\"]");

		logger.fine("clicking \"show more comments\"");
		expandPosts("//a[@class=\"UFIPagerLink\"]", false);

		logger.fine("expanding longer comments");
		clickIfPresent("//span[@class=\"UFICommentBody\"]/a[@class=\"_5v47 fss\"]");

		// Wait for page to load and fetch source code. Firefox sometimes stops responding when the source of a large
		// page is requested. We used to retry twice, but since Selenium only realizes the problem after a 3 hour
		// timeout, which cannot be changed to a more reasonable value, we now abort after the first exception.
		logger.fine("fetching HTML source");
		waitForPageComplete();
		String htmlSource = driver.getPageSource();
		pageSource.put("https://www.facebook.com/" + userId, htmlSource);

		Set<String> unresolvedParticipants = new HashSet<String>();
		Set<String> unresolvedLikes = new HashSet<String>();
		Set<String> unresolvedCoworkers = new HashSet<String>();

		logger.fine("analyzing posts");
		// matches regular posts, individual small posts, and life events
		elements = driver.findElements(By.xpath("//div[starts-with(@class, \"userContentWrapper\")]"));
		for (WebElement e : elements)
			handleTimelineElement(e, userId, unresolvedParticipants, unresolvedLikes);

		logger.fine("fetching additional pages");
		fetchPageByUserId(userId, "friends", pageSource);
		fetchMorePages("m_more_friends", pageSource);

		// extract personal data and referenced users
		fetchPageByUserId(userId, "info", pageSource);
		handlePersonDetails(unresolvedCoworkers);

		// extract site likes
		fetchPageByUserId(userId, "likes", pageSource);
		handleSiteLikes(pageSource);

		logger.fine("fetching full lists");
		for (String url : unresolvedParticipants)
			fetchPage(url, true, pageSource);
		for (String url : unresolvedLikes) {
			fetchPage(url, false, pageSource);
			fetchMorePages("m_more_item", pageSource);
		}
		for (String url : unresolvedCoworkers) {
			fetchPage(url, false, pageSource);
			// TODO: unknown pagination mechanism, need a user with more than ~20 coworkers to find out
		}

		logger.info("finished scraping profile");
	}


	// public API

	public FirefoxCrawler() {
		FirefoxProfile browserProfile = new FirefoxProfile();
		browserProfile.setPreference("dom.max_script_run_time", 2 * defaultTimeoutSeconds);
		browserProfile.setPreference("dom.max_chrome_script_run_time", 2 * defaultTimeoutSeconds);
		browserProfile.setPreference("permissions.default.image", 2);	// disable image loading
		browserProfile.setPreference("layout.css.devPixelsPerPx", "1.0");	// disable HiDPI scaling
		browserProfile.setPreference("layout.css.dpi", "96");

		driver = new FirefoxDriver(browserProfile);
		driver.manage().timeouts().pageLoadTimeout(defaultTimeoutSeconds, TimeUnit.SECONDS);
		driver.manage().timeouts().setScriptTimeout(defaultTimeoutSeconds, TimeUnit.SECONDS);
		js = (JavascriptExecutor) driver;

		// Facebook applies a different style if the window is smaller than 1024x768, so ensure that the window size is
		// consistently small.
		Dimension targetSize = new Dimension(900, 700);
		driver.manage().window().setSize(targetSize);
		Dimension actualSize = driver.manage().window().getSize();
		if (!actualSize.equals(targetSize)) {
			// At least on MacOS, if HiDPI scaling is enabled, setSize will transform the parameters, but getSize will
			// return the size in actual device pixels...
			float scaleX = (float) targetSize.getWidth() / actualSize.getWidth();
			float scaleY = (float) targetSize.getHeight() / actualSize.getHeight();
			driver.manage().window().setSize(new Dimension((int) (targetSize.getWidth() * scaleX),
					(int) (targetSize.getHeight() * scaleY)));
		}
	}

	public boolean login(String email, String pass) {
		driver.get("https://www.facebook.com/login.php");
		WebElement e = driver.findElement(By.id("email"));
		e.sendKeys(email);
		e = driver.findElement(By.id("pass"));
		e.sendKeys(pass);
		e = driver.findElement(By.id("u_0_1"));
		e.click();

		// check if login was successful
		driver.get("https://www.facebook.com/settings");
		boolean success = true;
		try {
			driver.findElement(By.xpath("//a[@class=\"item clearfix\"]"));
		} catch (NoSuchElementException ex) {
			success = false;
		}
		return success;
	}

	public Map<String, String> scrapeProfile(String userId) {
		String url = "https://www.facebook.com/" + userId;
		driver.get(url);
		waitForPageComplete();

		Map<String, String> pageSource = new HashMap<String, String>();
		String userName;
		try {
			WebElement e = driver.findElement(By.xpath("//div[@id=\"fbProfileCover\"]//a[@class=\"_8_2\"]"));
			userName = e.getText();
		} catch (NoSuchElementException ex) {
			logger.log(Level.WARNING, "error fetching user name, skipping");
			return null;	// abort, user does not exist or profile layout changed
		} finally {
			pageSource.put(url, driver.getPageSource());
		}

		logger.info("start scraping profile of user " + userName +" (id=" + userId + ")");
		scrapeCurrentProfile(userId, userName, pageSource);
		return pageSource;
	}

	public void shutdown() {
		try {
			driver.quit();
		} catch (UnreachableBrowserException ex) {
			logger.warning("browser crashed, unable to shut down cleanly");
		}
	}

}
