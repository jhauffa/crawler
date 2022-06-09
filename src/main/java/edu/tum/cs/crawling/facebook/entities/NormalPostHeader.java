package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

public class NormalPostHeader implements Serializable {

	private static final long serialVersionUID = -996589354086915322L;

	public static enum Type {
		UNKNOWN, NONE, POSTONTO, LIKESLINK, POSTLINK, COVERPHOTOCHANGE, PROFILEPICCHANGE, LINKVIA, SHAREDPHOTO,
		SHAREDVIDEO, WASONTHEROAD, UPLOADEDVIDEO, WASHERE, WASHEREWITH, WASINWITH, RECOMMENDSLINK, ADDPHOTOSTOALBUM,
		FRIENDSWITH, SHAREDALBUM, UPLOADEDPHOTO, WASWITH
	}

	private final String id;
	private final Type type;
	private final Person sender;
	private final Date time;
	private Place place;

	/** may be null even if type == SHARED*, e.g. if profile owner shared own content, or origin is a page */
	private Person sharedVia;

	private final Set<Person> persons;

	public NormalPostHeader(String id, Person sender) {
		this(id, Type.UNKNOWN, sender, null, null, null, null);
	}

	public NormalPostHeader(String id, Type type, Person sender, Date time, Place place, Person sharedVia,
			Set<Person> persons) {
		this.id = id;
		this.type = type;
		this.sender = sender;
		this.time = time;
		this.place = place;
		this.sharedVia = sharedVia;
		this.persons = persons;
	}

	public String getId() {
		return id;
	}

	public Type getType() {
		return type;
	}

	public Person getSender() {
		return sender;
	}

	public Date getTime() {
		return time;
	}

	public Place getPlace() {
		return place;
	}

	public void setPlace(Place place) {
		this.place = place;
	}

	public Person getSharedVia() {
		return sharedVia;
	}

	public void setSharedVia(Person sharedVia) {
		this.sharedVia = sharedVia;
	}

	public Set<Person> getPersons() {
		return persons;
	}

}
