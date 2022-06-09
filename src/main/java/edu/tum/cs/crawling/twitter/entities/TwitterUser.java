package edu.tum.cs.crawling.twitter.entities;

import java.util.Date;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.ColumnTransformer;

@Entity
@Table(name="USER", indexes = {
		@Index(columnList = "CRAWLING_FAILED,IGNORED,SECURED", name = "filter_index"),
})
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class TwitterUser extends TwitterUserData {

	private static final long serialVersionUID = 9154854390102238414L;

	@Column(name="SCREEN_NAME")
	private String screenName;

	@Column(name="NAME")
	private String name;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CREATED_AT")
	private Date createdAt;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CRAWLED_AT", nullable=false)
	private Date crawledAt = new Date();

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="FIRST_CRAWLED_AT", nullable=false)
	private Date firstCrawledAt = new Date();

	@Column(name="SECURED", nullable=false)
	private boolean secured = false;

	@Column(name="IGNORED", nullable=false)
	private boolean ignored = false;

	@Column(name="CRAWLING_FAILED", nullable=false)
	private boolean crawlingFailed = false;

	@Column(name="CRAWLING_FAILED_CAUSE")
	private String crawlingFailedCause;

	@Column(name="DETECTED_LANGUAGE", length=2)
	private String detectedLanguage;

	@Column(name="TOTAL_FOLLOWER_COUNT", nullable=false)
	private int totalFollowerCount = -1;

	@Column(name="TOTAL_FRIEND_COUNT", nullable=false)
	private int totalFriendCount = -1;

	@Column(name="TOTAL_TWEET_COUNT", nullable=false)
	private int totalTweetCount = -1;

	@Column(name="PREFERED_LANGUAGE")
	private String preferedLanguage;

	@Column(name="DESCRIPTION", columnDefinition="BLOB")
	@ColumnTransformer(read = "UNCOMPRESS(DESCRIPTION)", write = "COMPRESS(?)")
	private String description;

	@Column(name="LOCATION")
	private String location;

	@Column(name="UTC_OFFSET", nullable=false)
	private int utcOffset = 0;

	@Column(name="VERIFIED", nullable=false)
	private boolean verified = false;

	@Column(name="GEO_ENABLED", nullable=false)
	private boolean geoEnabled = false;

	@Column(name="CONTRIBUTORS_ENABLED", nullable=false)
	private boolean contributorsEnabled = false;

	@Transient
	private FollowersFriends followersFriends = new FollowersFriends();

	@Override
	public void setId(long userId) {
		super.setId(userId);
		followersFriends.setId(userId);
	}

	public int getTotalFollowerCount() {
		return totalFollowerCount;
	}

	public void setTotalFollowerCount(int totalFollowerCount) {
		this.totalFollowerCount = totalFollowerCount;
	}

	public int getTotalFriendCount() {
		return totalFriendCount;
	}

	public void setTotalFriendCount(int totalFriendCount) {
		this.totalFriendCount = totalFriendCount;
	}

	public int getTotalTweetCount() {
		return totalTweetCount;
	}

	public void setTotalTweetCount(int totalTweetCount) {
		this.totalTweetCount = totalTweetCount;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public boolean isSecured() {
		return secured;
	}

	public void setSecured(boolean secured) {
		this.secured = secured;
	}

	public String getDetectedLanguage() {
		return detectedLanguage;
	}

	public void setDetectedLanguage(String detectedLanguage) {
		if ((detectedLanguage != null) && (detectedLanguage.length() >= 2))
			detectedLanguage = detectedLanguage.substring(0, 2);
		this.detectedLanguage = detectedLanguage;
	}

	public Date getCrawledAt() {
		return crawledAt;
	}

	public void setCrawledAt(Date crawledAt) {
		this.crawledAt = crawledAt;
	}

	public void setUtcOffset(int utcOffset) {
		this.utcOffset = utcOffset;
	}

	public int getUtcOffset() {
		return utcOffset;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getLocation() {
		return location;
	}

	public void setPreferedLanguage(String lang) {
		this.preferedLanguage = lang;
	}

	public String getPreferedLanguage() {
		return preferedLanguage;
	}

	public boolean isIgnored() {
		return ignored;
	}

	public void setIgnored(boolean ignored) {
		this.ignored = ignored;
	}

	public boolean isCrawlingFailed() {
		return crawlingFailed;
	}

	public void setCrawlingFailed(boolean crawlingFailed) {
		this.crawlingFailed = crawlingFailed;
	}

	public String getCrawlingFailedCause() {
		return crawlingFailedCause;
	}

	public void setCrawlingFailedCause(String crawlingFailedCause) {
		if ((crawlingFailedCause != null) && (crawlingFailedCause.length() >= 255))
			crawlingFailedCause = crawlingFailedCause.substring(0, Math.min(254, crawlingFailedCause.length()));
		this.crawlingFailedCause = crawlingFailedCause;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public boolean getVerified() {
		return verified;
	}

	public void setVerified(boolean verified) {
		this.verified = verified;
	}

	public boolean isContributorsEnabled() {
		return contributorsEnabled;
	}

	public void setContributorsEnabled(boolean contributorsEnabled) {
		this.contributorsEnabled = contributorsEnabled;
	}

	public boolean isGeoEnabled() {
		return geoEnabled;
	}

	public void setGeoEnabled(boolean geoEnabled) {
		this.geoEnabled = geoEnabled;
	}

	public Date getFirstCrawledAt() {
		return firstCrawledAt;
	}

	public void setFirstCrawledAt(Date firstCrawledAt) {
		this.firstCrawledAt = firstCrawledAt;
	}

	public FollowersFriends getFollowersFriends() {
		return followersFriends;
	}

	public static TwitterUser fromTwitter4j(twitter4j.User twitter4jUser, Date crawledAt, String detectedLanguage) {
		TwitterUser user = new TwitterUser();

		user.setId(twitter4jUser.getId());

		user.setCrawledAt(crawledAt);
		user.setCreatedAt(twitter4jUser.getCreatedAt());

		user.setTotalFollowerCount(twitter4jUser.getFollowersCount());
		user.setTotalFriendCount(twitter4jUser.getFriendsCount());
		user.setTotalTweetCount(twitter4jUser.getStatusesCount());

		user.setPreferedLanguage(twitter4jUser.getLang());
		user.setLocation(twitter4jUser.getLocation());
		user.setUtcOffset(twitter4jUser.getUtcOffset());

		user.setVerified(twitter4jUser.isVerified());

		user.setDescription(twitter4jUser.getDescription());

		user.setScreenName(twitter4jUser.getScreenName());
		user.setName(twitter4jUser.getName());

		user.setDetectedLanguage(detectedLanguage);

		user.setSecured(twitter4jUser.isProtected());
		user.setGeoEnabled(twitter4jUser.isGeoEnabled());
		user.setContributorsEnabled(twitter4jUser.isContributorsEnabled());

		return user;
	}

}
