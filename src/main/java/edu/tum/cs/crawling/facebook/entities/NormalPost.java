package edu.tum.cs.crawling.facebook.entities;

import java.util.Set;

public class NormalPost extends Post {

	private static final long serialVersionUID = 6965000600678825929L;

	protected final NormalPostHeader header;
	private final String usercontent;
	private EmbeddableObject embeddedcontent;
	private final Set<Person> mentionedPersons;
	private final Set<Site> mentionedSites;

	public NormalPost(NormalPostHeader header, String usercontent, EmbeddableObject embeddedcontent,
			Set<Person> mentionedPersons, Set<Site> mentionedSites) {
		this.header = header;
		this.usercontent = usercontent;
		this.embeddedcontent = embeddedcontent;
		this.mentionedPersons = mentionedPersons;
		this.mentionedSites = mentionedSites;
	}

	public NormalPostHeader getHeader() {
		return header;
	}

	@Override
	public String getText() {
		return usercontent;
	}

	public EmbeddableObject getEmbeddedContent() {
		return embeddedcontent;
	}

	public void setEmbeddedContent(EmbeddableObject embeddedcontent) {
		this.embeddedcontent = embeddedcontent;
	}

	public Set<Person> getMentionedPersons() {
		return mentionedPersons;
	}

	public Set<Site> getMentionedSites() {
		return mentionedSites;
	}

}
