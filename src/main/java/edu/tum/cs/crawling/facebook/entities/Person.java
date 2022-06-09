package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class Person implements Serializable {

	private static final long serialVersionUID = -1284299230820906828L;

	private final String id;
	private final String name;

	public Person(String id, String name) {
		this.id = id;
		this.name = name;
	}

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
		if (!(other instanceof Person))
			return false;
		return id.equals(((Person) other).id);
	}

}
