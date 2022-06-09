package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class PhotoAlbum implements Serializable, EmbeddableObject {

	private static final long serialVersionUID = -2121462986307193704L;

	private final String id;
	private final String name;
	private final String shareComment;

	public PhotoAlbum(String id, String name, String shareComment) {
		this.id = id;
		this.name = name;
		this.shareComment = shareComment;
	}

	/**
	 * @return album ID; URL is constructed as follows: https://www.facebook.com/media/set/?set=ID&type=1&stream_ref=10
	 * 	prefix specifies album type: "a." regular album, "ms." subset of album (individual photo IDs encoded into album
	 * 	ID), "at." tagged photos
	 */
	@Override
	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getShareComment() {
		return shareComment;
	}

}
