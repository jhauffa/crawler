package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Post implements Serializable {

	private static final long serialVersionUID = 6837273898846909356L;

	private final List<Comment> comments = new ArrayList<Comment>();
	private final Set<Person> likes = new HashSet<Person>();

	public abstract String getText();

	public List<Comment> getComments() {
		return comments;
	}

	public Set<Person> getLikes() {
		return likes;
	}

}
