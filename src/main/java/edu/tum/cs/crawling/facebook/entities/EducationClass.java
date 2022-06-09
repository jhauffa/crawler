package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Set;

public class EducationClass implements Serializable {

	private static final long serialVersionUID = 2117841620806847653L;

	private final String name;
	private final String description;
	private final Set<Person> persons;

	public EducationClass(String name, String description, Set<Person> persons) {
		this.name = name;
		this.description = description;
		this.persons = persons;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public Set<Person> getPersons() {
		return persons;
	}

}
