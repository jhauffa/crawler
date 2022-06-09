package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Video implements Serializable, EmbeddableObject {

	private static final long serialVersionUID = -2477817181596909414L;

	private final String id;	// https://www.facebook.com/photo.php?v=ID
	private final String title;
	private final String shareComment;

	public Video(String id, String title, String shareComment) {
		this.id = id;
		this.title = title;
		this.shareComment = shareComment;
	}

	@Override
	public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public String getShareComment() {
		return shareComment;
	}

}
