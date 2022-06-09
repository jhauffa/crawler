package edu.tum.cs.crawling.facebook.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class PersonDetails implements Serializable {

	private static final long serialVersionUID = 1850447894476011363L;

	private final String bio;
	private final String quotes;
	private final char gender;
	private final char interestedin;
	private final Date birthdate;
	private final Set<String> spokenlanguages;
	private final String religion;
	private final String religionid;
	private final String politics;
	private final String politicsid;
	private final String telephonenumber;
	private final String address;
	private final String homepage;
	private final String email;
	private final String relationshipstatus;
	private final String currentresidence;
	private final String currentresidenceid;
	private final String hometown;
	private final String hometownid;

	private final Set<FamilyMember> family;	/** family members without a Facebook account are not extracted */
	private final List<EducationItem> education;
	private final List<WorkItem> work;

	public PersonDetails(String bio, String quotes, char gender, char interestedin, Date birthdate,
			Set<String> spokenlanguages, String religion, String religionid, String politics, String politicsid,
			String telephonenumber, String address, String homepage, String email, String relationshipstatus,
			String currentresidence, String currentresidenceid, String hometown, String hometownid,
			Set<FamilyMember> family, List<EducationItem> education, List<WorkItem> work) {
		this.bio = bio;
		this.quotes = quotes;
		this.gender = gender;
		this.interestedin = interestedin;
		this.birthdate = birthdate;
		this.spokenlanguages = spokenlanguages;
		this.religion = religion;
		this.religionid = religionid;
		this.politics = politics;
		this.politicsid = politicsid;
		this.telephonenumber = telephonenumber;
		this.address = address;
		this.homepage = homepage;
		this.email = email;
		this.relationshipstatus = relationshipstatus;
		this.currentresidence = currentresidence;
		this.currentresidenceid = currentresidenceid;
		this.hometown = hometown;
		this.hometownid = hometownid;
		this.family = family;
		this.education = education;
		this.work = work;
	}

	public String getBio() {
		return bio;
	}

	public String getQuotes() {
		return quotes;
	}

	public char getGender() {
		return gender;
	}

	public char getInterestedin() {
		return interestedin;
	}

	public Date getBirthdate() {
		return birthdate;
	}

	public Set<String> getSpokenlanguages() {
		return spokenlanguages;
	}

	public String getReligion() {
		return religion;
	}

	public String getReligionid() {
		return religionid;
	}

	public String getPolitics() {
		return politics;
	}

	public String getPoliticsid() {
		return politicsid;
	}

	public String getTelephonenumber() {
		return telephonenumber;
	}

	public String getAddress() {
		return address;
	}

	public String getHomepage() {
		return homepage;
	}

	public String getEmail() {
		return email;
	}

	public String getRelationshipstatus() {
		return relationshipstatus;
	}

	public String getCurrentresidence() {
		return currentresidence;
	}

	public String getCurrentresidenceid() {
		return currentresidenceid;
	}

	public String getHometown() {
		return hometown;
	}

	public String getHometownid() {
		return hometownid;
	}

	public Set<FamilyMember> getFamily() {
		return family;
	}

	public List<EducationItem> getEducation() {
		return education;
	}

	public List<WorkItem> getWork() {
		return work;
	}

	public float assessCompleteness() {
		int numDetails = 0;
		if (!address.isEmpty())
			numDetails++;
		if (!bio.isEmpty())
			numDetails++;
		if (birthdate != null)
			numDetails++;
		if (!currentresidenceid.isEmpty())
			numDetails++;
		if (!education.isEmpty())
			numDetails++;
		if (!email.isEmpty())
			numDetails++;
		if (!family.isEmpty())
			numDetails++;
		if (gender != 'u')
			numDetails++;
		if (!homepage.isEmpty())
			numDetails++;
		if (!hometownid.isEmpty())
			numDetails++;
		if (interestedin != 'u')
			numDetails++;
		if (!politicsid.isEmpty())
			numDetails++;
		if (!quotes.isEmpty())
			numDetails++;
		if (!relationshipstatus.isEmpty())
			numDetails++;
		if (!religionid.isEmpty())
			numDetails++;
		if (!spokenlanguages.isEmpty())
			numDetails++;
		if (!telephonenumber.isEmpty())
			numDetails++;
		if (!work.isEmpty())
			numDetails++;
		return (float) numDetails / 18;
	}

}
