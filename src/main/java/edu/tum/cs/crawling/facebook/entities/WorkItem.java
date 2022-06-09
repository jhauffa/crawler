package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.List;

public class WorkItem implements Serializable {

	private static final long serialVersionUID = -1904802971431315038L;

	private final String name;
	private final String id;
	private final String title;
	private final String timeperiod;
	private final String place;
	private final String description;
	private final List<WorkProject> projects;

	public WorkItem(String name, String id, String title, String timeperiod, String place, String description,
			List<WorkProject> projects) {
		this.name = name;
		this.id = id;
		this.title = title;
		this.timeperiod = timeperiod;
		this.place = place;
		this.description = description;
		this.projects = projects;
	}

	public String getName() {
		return name;
	}

	public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public String getTimeperiod() {
		return timeperiod;
	}

	public String getPlace() {
		return place;
	}

	public String getDescription() {
		return description;
	}

	public List<WorkProject> getProjects() {
		return projects;
	}

}
