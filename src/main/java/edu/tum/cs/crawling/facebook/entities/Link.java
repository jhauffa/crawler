package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Link implements Serializable, EmbeddableObject {

	private static final long serialVersionUID = 7293675725229064977L;

	private final String url;

	public Link(String url) {
		this.url = url;
	}

	@Override
	public String getId() {
		// synthesize a unique ID; hash collisions will cause an exception on insertion into the database
		return Integer.toString(url.hashCode());
	}

	public String getUrl() {
		return url;
	}

}
