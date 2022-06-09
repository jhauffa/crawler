package edu.tum.cs.postprocessing;

public class HackingTeamAliasList extends UserAliasList {

	@Override
	public String normalize(String name) {
		name = name.toLowerCase();
		if (name.endsWith("@hackingteam.com") || name.endsWith("@hackingteam.it"))
			name = name.substring(0, name.indexOf('@')) + "@hackingteam";
		return name;
	}

}
