package edu.tum.cs.crawling.twitter.entities;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.io.Serializable;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

@Entity
@Table(name = "WEBSITE",
	uniqueConstraints = {
			@UniqueConstraint(columnNames = { "TWEET_ID", "ORIGINAL_URL" }, name = "TWEET_ID_ORIGINAL_URL_INDEX") },
	indexes = { @Index(columnList = "TWEET_ID", name = "TWEET_ID_INDEX") }
)
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Website implements Serializable {

	private static final long serialVersionUID = -7539306184716532883L;

	private long id;
	private String content;
	private String originalUrl;
	private long tweetId;
	private String resolvedUrl;
	private int statusCode;

	@Id
	@Column(name = "ID")
	@GeneratedValue(strategy=GenerationType.AUTO)
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Basic
	@Lob
	@Column(name = "CONTENT")
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Basic
	/* hack: need to override the text encoding so that the combined size of ORINGIAL_URL and TWEET_ID is below the
	   maximum for a uniqueness constraint */
	@Column(name = "ORIGINAL_URL", nullable=false, columnDefinition = "VARCHAR(255) CHARSET UTF8")
	public String getOriginalUrl() {
		return originalUrl;
	}

	public void setOriginalUrl(String originalUrl) {
		this.originalUrl = originalUrl;
	}

	@Basic
	@Column(name = "TWEET_ID", nullable=false)
	public long getTweetId() {
		return tweetId;
	}

	public void setTweetId(long tweetId) {
		this.tweetId = tweetId;
	}

	@Basic
	@Lob
	@Column(name = "RESOLVED_URL")
	public String getResolvedUrl() {
		return resolvedUrl;
	}

	public void setResolvedUrl(String resolvedUrl) {
		this.resolvedUrl = resolvedUrl;
	}

	@Basic
	@Column(name = "STATUS_CODE")
	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Website website = (Website) o;

		if (id != website.id) return false;
		if (statusCode != website.statusCode) return false;
		if (tweetId != website.tweetId) return false;
		if (content != null ? !content.equals(website.content) : website.content != null) return false;
		if (originalUrl != null ? !originalUrl.equals(website.originalUrl) : website.originalUrl != null) return false;
		if (resolvedUrl != null ? !resolvedUrl.equals(website.resolvedUrl) : website.resolvedUrl != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (id ^ (id >>> 32));
		result = 31 * result + (content != null ? content.hashCode() : 0);
		result = 31 * result + (originalUrl != null ? originalUrl.hashCode() : 0);
		result = 31 * result + (int) (tweetId ^ (tweetId >>> 32));
		result = 31 * result + (resolvedUrl != null ? resolvedUrl.hashCode() : 0);
		result = 31 * result + statusCode;
		return result;
	}

}
