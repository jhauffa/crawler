package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Set;

public class WorkProject implements Serializable {

	private static final long serialVersionUID = -4525759841237518810L;

	private final String name;
	private final String timeperiod;
	private final String description;
	private final Set<Person> persons;

	public WorkProject(String name, String timeperiod, String description, Set<Person> persons) {
		this.name = name;
		this.timeperiod = timeperiod;
		this.description = description;
		this.persons = persons;
	}

	public String getName() {
		return name;
	}

	public String getTimeperiod() {
		return timeperiod;
	}

	public String getDescription() {
		return description;
	}

	public Set<Person> getPersons() {
		return persons;
	}

}
