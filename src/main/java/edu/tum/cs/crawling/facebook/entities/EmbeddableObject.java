package edu.tum.cs.crawling.facebook.entities;

/**
 * Any entity that has a unique ID, can be embedded into the body of a NormalPost, and can be shared. Notable exceptions
 * are PhotoAlbum, which cannot be shared, and Link, which does not have a (visible) ID.
 */
public interface EmbeddableObject {

	public String getId();

}
