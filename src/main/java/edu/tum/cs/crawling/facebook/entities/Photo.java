package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Photo implements Serializable, EmbeddableObject {

	private static final long serialVersionUID = -4827875087437382885L;

	private final String id;
	private final String albumId;
	private final String shareComment;

	public Photo(String id, String albumId, String shareComment) {
		this.id = id;
		this.albumId = albumId;
		this.shareComment = shareComment;
	}

	@Override
	public String getId() {
		return id;
	}

	public String getAlbumId() {
		return albumId;
	}

	/**
	 * When a photo is shared, a part of the original post is "quoted" above the photo. Since we do not extract the
	 * full original post, we store this text with the photo.
	 * @return an extract from the original post containing the shared photo; may be an empty string if the photo was
	 * 	originally posted (i.e. not shared) or the original post does not contain any text
	 */
	public String getShareComment() {
		return shareComment;
	}

}
