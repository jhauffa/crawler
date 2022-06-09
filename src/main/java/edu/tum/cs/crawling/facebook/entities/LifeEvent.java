package edu.tum.cs.crawling.facebook.entities;

import java.util.Date;

public class LifeEvent extends Post {

	private static final long serialVersionUID = -2680688729906822265L;

	private final String title;
	private final Date time;
	private final String subtitle;
	private final String text;

	public LifeEvent(String title, Date time, String subtitle, String text) {
		this.title = title;
		this.time = time;
		this.subtitle = subtitle;
		this.text = text;
	}

	public String getTitle() {
		return title;
	}

	public Date getTime() {
		return time;
	}

	public String getSubtitle() {
		return subtitle;
	}

	@Override
	public String getText() {
		return text;
	}

}
