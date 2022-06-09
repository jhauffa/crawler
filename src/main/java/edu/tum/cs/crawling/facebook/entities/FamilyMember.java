package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;

public class FamilyMember implements Serializable {

	private static final long serialVersionUID = 3848286344180525507L;

	private final Person person;
	private final String relation;

	public FamilyMember(Person person, String relation) {
		this.person = person;
		this.relation = relation;
	}

	public Person getPerson() {
		return person;
	}

	public String getRelation() {
		return relation;
	}

	@Override
	public int hashCode() {
		return person.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof FamilyMember))
			return false;
		return person.equals(((FamilyMember) other).person);
	}

}
