package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Date;

public class SmallPostHeader implements Serializable {

	private static final long serialVersionUID = -3098595724720993948L;

	private final Person sender;
	private final Date time;

	public SmallPostHeader(Person sender, Date time) {
		this.sender = sender;
		this.time = time;
	}

	public Person getSender() {
		return sender;
	}

	public Date getTime() {
		return time;
	}

}
