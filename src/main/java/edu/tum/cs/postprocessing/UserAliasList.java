package edu.tum.cs.postprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;

public abstract class UserAliasList {

	private static final Logger logger = Logger.getLogger(UserAliasList.class.getName());

	private final Map<String, Set<String>> canonicalToAlias = new TreeMap<String, Set<String>>();
	private final Map<String, String> aliasToCanonical = new HashMap<String, String>();

	public abstract String normalize(String name);

	public String resolve(String aliasName) {
		return aliasToCanonical.get(normalize(aliasName));
	}

	public void add(String canonicalName, Collection<String> aliasNames) {
		canonicalName = normalize(canonicalName);
		Set<String> names = canonicalToAlias.get(canonicalName);
		if (names == null) {
			names = new HashSet<String>();
			canonicalToAlias.put(canonicalName, names);
		}

		for (String aliasName : aliasNames) {
			aliasName = normalize(aliasName);
			if (!aliasName.equals(canonicalName)) {
				String prevCanonicalName = aliasToCanonical.get(aliasName);
				if (prevCanonicalName == null) {
					aliasToCanonical.put(aliasName, canonicalName);
					names.add(aliasName);
				} else if (!prevCanonicalName.equals(canonicalName))
					throw new IllegalStateException("alias '" + aliasName + "' is already mapped to '" +
							prevCanonicalName + "'");
			}
		}
	}

	public void merge(Collection<String> aliasNames) {
		Set<String> matchingCanonicalNames = new HashSet<String>();
		for (String aliasName : aliasNames) {
			String canonicalName = resolve(aliasName);
			if (canonicalName != null)
				matchingCanonicalNames.add(canonicalName);
		}

		String canonicalName;
		if (!matchingCanonicalNames.isEmpty()) {
			if (matchingCanonicalNames.size() > 1)
				canonicalName = mergeCanonicalNames(matchingCanonicalNames);
			else
				canonicalName = matchingCanonicalNames.iterator().next();
		} else
			canonicalName = "!" + UUID.randomUUID().toString();
		add(canonicalName, aliasNames);
	}

	private String mergeCanonicalNames(Collection<String> canonicalNames) {
		String targetName = null;
		for (String name : canonicalNames) {
			if (targetName == null)
				targetName = name;
			else if (!name.startsWith("!")) {
				if (targetName.startsWith("!"))
					targetName = name;
				else
					logger.warning("merging custodians '" + targetName + "' and '" + name + "'");
			}
		}

		Set<String> targetSet = canonicalToAlias.get(targetName);
		for (String name : canonicalNames) {
			if (!name.equals(targetName)) {
				Set<String> aliasNames = canonicalToAlias.get(name);
				targetSet.addAll(aliasNames);
				for (String aliasName : aliasNames)
					aliasToCanonical.put(aliasName, targetName);
				canonicalToAlias.remove(name);
			}
		}
		return targetName;
	}

	public void clean() {
		Iterator<Map.Entry<String, Set<String>>> it = canonicalToAlias.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Set<String>> e = it.next();
			if (e.getKey().startsWith("!") && (e.getValue().size() == 1)) {
				String aliasName = e.getValue().iterator().next();
				aliasToCanonical.remove(aliasName);
				it.remove();
			}
		}
	}

	public boolean hasAliases(String canonicalName) {
		return canonicalToAlias.containsKey(canonicalName);
	}

	public Map<String, Set<String>> getAliases() {
		return canonicalToAlias;
	}

	public void readFromStream(InputStream is) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#"))
					continue;
				int pos = line.indexOf(" : ");
				if (pos < 0)
					continue;
				String canonicalName = line.substring(0, pos).replace(" ", "-");
				String[] aliasNames = line.substring(pos + 3).split(",");
				add(canonicalName, Arrays.asList(aliasNames));
			}
		} finally {
			reader.close();
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Set<String>> e : canonicalToAlias.entrySet()) {
			if (!e.getValue().isEmpty()) {
				sb.append(e.getKey()).append(" : ");
				for (String s : e.getValue())
					sb.append(s).append(",");
				sb.append('\n');
			}
		}
		return sb.toString();
	}

}
