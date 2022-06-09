package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Site implements Serializable {

	private static final long serialVersionUID = -9148706903085939625L;

	private final String id;
	private final String name;

	public Site(String id, String name) {
		this.id = id;
		this.name = name;
	}

	/**
	 * @return site ID, so that the site can be fetched as www.facebook.com/(id)
	 */
	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof Site))
			return false;
		return id.equals(((Site) other).id);
	}

}
