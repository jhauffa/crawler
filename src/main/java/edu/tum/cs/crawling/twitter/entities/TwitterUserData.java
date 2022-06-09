package edu.tum.cs.crawling.twitter.entities;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

@MappedSuperclass
public abstract class TwitterUserData implements Serializable {

	private static final long serialVersionUID = 572640123431623828L;

	@Id
	@Column(name="ID", nullable=false)
	private long id;

	public long getId() {
		return id;
	}

	public void setId(long userId) {
		this.id = userId;
	}

	@Override
	public boolean equals(Object o) {
		if ((o != null) && (o instanceof TwitterUserData))
			return (getId() == ((TwitterUserData) o).getId());
		return false;
	}

	@Override
	public int hashCode() {
		return Long.valueOf(id).hashCode();
	}

}
