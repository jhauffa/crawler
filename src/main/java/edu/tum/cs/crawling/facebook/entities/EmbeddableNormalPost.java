package edu.tum.cs.crawling.facebook.entities;

import java.util.Set;

public class EmbeddableNormalPost extends NormalPost implements EmbeddableObject {

	private static final long serialVersionUID = 9168702944694286382L;

	public EmbeddableNormalPost(NormalPostHeader header, String usercontent, EmbeddableObject embeddedcontent,
			Set<Person> mentionedPersons, Set<Site> mentionedSites) {
		super(header, usercontent, embeddedcontent, mentionedPersons, mentionedSites);
	}

	@Override
	public String getId() {
		return header.getId();
	}

	@Override
	public int hashCode() {
		return header.getId().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof EmbeddableNormalPost))
			return false;
		EmbeddableNormalPost other = (EmbeddableNormalPost) o;
		return header.getId().equals(other.header.getId());
	}

}
