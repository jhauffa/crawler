package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Place implements Serializable {

	private static final long serialVersionUID = 5111793776550520598L;

	private final String id;
	private final String name;

	public Place(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

}
