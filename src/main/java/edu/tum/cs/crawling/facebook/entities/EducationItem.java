package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.List;

public class EducationItem implements Serializable {

	private static final long serialVersionUID = -3015508852280255708L;

	private final String name;
	private final String id;
	private final String timeperiod;
	private final String type;
	private final String field;
	private final List<EducationClass> classes;

	public EducationItem(String name, String id, String timeperiod, String type, String field,
			List<EducationClass> classes) {
		this.name = name;
		this.id = id;
		this.timeperiod = timeperiod;
		this.type = type;
		this.field = field;
		this.classes = classes;
	}

	public String getName() {
		return name;
	}

	public String getId() {
		return id;
	}

	public String getTimeperiod() {
		return timeperiod;
	}

	/**
	 * Returns one of a supposedly small number of strings identifying the education type, e.g. "Schule" or
	 * "Hochschule". If the user filled in only one of the fields "type" and "subtype" in his profile, it is impossible
	 * to properly identify the field, so this method may return the subtype in some cases. This can be worked around
	 * after crawling by collecting a list of Strings that are only ever returned by {@link #getType()}, never by
	 * {@link #getSubtype()}, and treating them as the canonical types.
	 */
	public String getType() {
		return type;
	}

	public String getField() {
		return field;
	}

	public List<EducationClass> getClasses() {
		return classes;
	}

}
