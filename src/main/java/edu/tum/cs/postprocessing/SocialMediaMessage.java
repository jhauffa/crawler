package edu.tum.cs.postprocessing;

import java.util.Date;
import java.util.Set;

public class SocialMediaMessage {

	private final String id;
	private final Date date;
	private final SocialMediaUser sender;
	private final Set<SocialMediaUser> recipients;
	private boolean isReply;
	private SocialMediaMessage parent;
	private boolean isShared;
	private SocialMediaMessage origin;
	private String content;

	public SocialMediaMessage(String id, Date date, SocialMediaUser sender, Set<SocialMediaUser> recipients,
			boolean isReply, SocialMediaMessage parent, boolean isShared, SocialMediaMessage origin, String content) {
		this.id = id;
		this.date = date;
		this.sender = sender;
		this.recipients = recipients;
		this.isReply = isReply;
		this.parent = parent;
		this.isShared = isShared;
		this.origin = origin;
		this.content = content;
	}

	public SocialMediaMessage(String id, Date date, SocialMediaUser sender, Set<SocialMediaUser> recipients,
			String content) {
		this(id, date, sender, recipients, false, null, false, null, content);
	}

	public String getId() {
		return id;
	}

	public Date getDate() {
		return date;
	}

	public SocialMediaUser getSender() {
		return sender;
	}

	public Set<SocialMediaUser> getRecipients() {
		return recipients;
	}

	public boolean isReply() {
		return isReply;
	}

	public SocialMediaMessage getParent() {
		return parent;
	}

	public void updateParent(SocialMediaMessage parent) {
		isReply = true;
		this.parent = parent;
	}

	public boolean isShared() {
		return isShared;
	}

	public SocialMediaMessage getOrigin() {
		return origin;
	}

	public void updateOrigin(SocialMediaMessage origin) {
		isShared = true;
		this.origin = origin;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof SocialMediaMessage))
			return false;
		return id.equals(((SocialMediaMessage) other).id);
	}

}
