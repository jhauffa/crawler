package edu.tum.cs.crawling.twitter.entities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import twitter4j.Status;

@Entity
@Table(name="TWEET", indexes = {
		@Index(columnList = "USER_ID", name = "ind_user_id"),
		@Index(columnList = "RETWEET_OF_USER_ID", name = "ind_retweet_of_user_id"),
		@Index(columnList = "IN_REPLY_TO_USER_ID", name = "ind_in_reply_to_user_id"),
})
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Tweet implements Serializable {

	private static final long serialVersionUID = -6259055616470764348L;

	@Id
	@Column(name="ID", nullable=false)
	private long id;

	@Column(name="USER_ID", nullable=false)
	private long userId;

	@Column(name="CURRENT_SCREEN_NAME")
	private String currentScreenName;

	@Column(name="STATUS_TEXT")
	private String statusText;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CREATED_AT", nullable=false)
	private Date createdAt = new Date();

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="CRAWLED_AT", nullable=false)
	private Date crawledAt = new Date();

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="FIRST_CRAWLED_AT", nullable=false)
	private Date firstCrawledAt = new Date();

	@Column(name="IN_REPLY_TO_USER_ID")
	private long inReplyToUserId;

	@Column(name="IN_REPLY_TO_STATUS_ID")
	private long inReplyToStatusId;

	@Column(name="IN_REPLY_TO_SCREENNAME")
	private String inReplyToScreenname;

	@Column(name="IS_RETWEET", nullable=false)
	private boolean isRetweet = false;

	@Column(name="RETWEET_OF_STATUS_ID")
	private long retweetOfStatusId;

	@Column(name="RETWEET_OF_USER_ID")
	private long retweetOfUserId;

	@Column(name="RETWEET_OF_SCREEN_NAME")
	private String retweetOfScreenName;

	@Column(name="LONGITUDE")
	private double longitude;

	@Column(name="LATITUDE")
	private double latitude;

	@Column(name="RETWEET_COUNT", nullable=false)
	private long retweetCount = 0;

	/*
	@Column(name="JSON", columnDefinition="BLOB")
	@ColumnTransformer(read = "UNCOMPRESS(JSON)", write = "COMPRESS(?)")
	private String jsonSource;
	*/

	public long getId() {
		return id;
	}

	public void setId(long statusId) {
		this.id = statusId;
	}

	public String getStatusText() {
		return statusText;
	}

	public void setStatusText(String statusText) {
		this.statusText = statusText;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public long getInReplyToUserId() {
		return inReplyToUserId;
	}

	public void setInReplyToUserId(long inReplyToUserId) {
		this.inReplyToUserId = inReplyToUserId;
	}

	public long getInReplyToStatusId() {
		return inReplyToStatusId;
	}

	public void setInReplyToStatusId(long inReplyToStatusId) {
		this.inReplyToStatusId = inReplyToStatusId;
	}

	public String getInReplyToScreenname() {
		return inReplyToScreenname;
	}

	public void setInReplyToScreenname(String inReplyToScreenname) {
		this.inReplyToScreenname = inReplyToScreenname;
	}

	public boolean isRetweet() {
		return isRetweet;
	}

	public void setRetweet(boolean isRetweet) {
		this.isRetweet = isRetweet;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public long getRetweetCount() {
		return retweetCount;
	}

	public void setRetweetCount(long retweetCount) {
		this.retweetCount = retweetCount;
	}

	public long getRetweetOfUserId() {
		return retweetOfUserId;
	}

	public void setRetweetOfUserId(long retweetOfUserId) {
		this.retweetOfUserId = retweetOfUserId;
	}

	public long getRetweetOfStatusId() {
		return retweetOfStatusId;
	}

	public void setRetweetOfStatusId(long retweetOfStatusId) {
		this.retweetOfStatusId = retweetOfStatusId;
	}

	/*
	public String getJSONSource() {
		return jsonSource;
	}

	public void setJSONSource(String source) {
		this.jsonSource = source;
	}
	*/

	public Date getCrawledAt() {
		return crawledAt;
	}

	public void setCrawledAt(Date crawledAt) {
		this.crawledAt = crawledAt;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public String getRetweetOfScreenName() {
		return retweetOfScreenName;
	}

	public void setRetweetOfScreenName(String retweetOfScreenName) {
		this.retweetOfScreenName = retweetOfScreenName;
	}

	public String getCurrentScreenName() {
		return currentScreenName;
	}

	public void setCurrentScreenName(String currentScreenName) {
		this.currentScreenName = currentScreenName;
	}

	public Date getFirstCrawledAt() {
		return firstCrawledAt;
	}

	public void setFirstCrawledAt(Date firstCrawledAt) {
		this.firstCrawledAt = firstCrawledAt;
	}

	@Override
	public String toString() {
		return getStatusText();
	}

	@Override
	public boolean equals(Object o) {
		if ((o != null) && (o instanceof Tweet))
			return (getId() == ((Tweet) o).getId());
		return false;
	}

	@Override
	public int hashCode() {
		return Long.valueOf(id).hashCode();
	}


	/*
	 * Static factory methods
	 */

	public static Collection<Tweet> fromTwitter4j(Collection<Status> statusCol, Date crawledAt) {
		List<Tweet> tweets = new ArrayList<Tweet>();
		for (Status status : statusCol)
			tweets.addAll(fromTwitter4j(status, crawledAt));
		return tweets;
	}

	public static List<Tweet> fromTwitter4j(Status status, Date crawledAt) {
		List<Tweet> tweets = new ArrayList<Tweet>();

		Tweet tweet = new Tweet();
		tweet.setId(status.getId());
		tweet.setUserId(status.getUser().getId());
		tweet.setCurrentScreenName(status.getUser().getScreenName());

		tweet.setStatusText(status.getText());

		tweet.setCrawledAt(crawledAt);
		tweet.setCreatedAt(status.getCreatedAt());

		tweet.setInReplyToScreenname(status.getInReplyToScreenName());
		tweet.setInReplyToStatusId(status.getInReplyToStatusId());
		tweet.setInReplyToUserId(status.getInReplyToUserId());

		if (status.getGeoLocation() != null) {
			tweet.setLatitude(status.getGeoLocation().getLatitude());
			tweet.setLongitude(status.getGeoLocation().getLongitude());
		}

		// tweet.setJSONSource(DataObjectFactory.getRawJSON(status));

		tweet.setRetweet(status.isRetweet());
		tweet.setRetweetCount(status.getRetweetCount());

		if (status.getRetweetedStatus() != null) {
			tweet.setRetweetOfStatusId(status.getRetweetedStatus().getId());
			tweet.setRetweetOfUserId(status.getRetweetedStatus().getUser().getId());
			tweet.setRetweetOfScreenName(status.getRetweetedStatus().getUser().getScreenName());
			tweets.addAll(fromTwitter4j(status.getRetweetedStatus(), crawledAt));
		}

		tweets.add(tweet);
		return tweets;
	}

}
