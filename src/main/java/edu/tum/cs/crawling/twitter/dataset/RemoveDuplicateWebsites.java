package edu.tum.cs.crawling.twitter.dataset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.tum.cs.crawling.twitter.server.TwitterDao;

public class RemoveDuplicateWebsites {

	private static Set<Integer> findDuplicateContent(int minInst) throws SQLException {
		Map<Integer, Integer> hashToSingleId = new HashMap<Integer, Integer>();
		Map<Integer, List<Integer>> hashToIds = new HashMap<Integer, List<Integer>>();
		Set<Integer> seenResolvedUrls = new HashSet<Integer>();

		int numRead = 0;
		Connection c = TwitterDao.getConnection();
		try {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery("select ID,CONTENT,RESOLVED_URL from WEBSITE_FILTERED");
			while (rs.next()) {
				int id = rs.getInt(1);
				int contentHash = rs.getString(2).hashCode();

				// ignore the anchor part of the URL when checking for uniqueness
				String url = rs.getString(3);
				int idxAnchor = url.lastIndexOf('#');
				if ((idxAnchor > 0) && (idxAnchor > url.lastIndexOf('/')))
					url = url.substring(0, idxAnchor);
				int urlHash = url.hashCode();

				if (seenResolvedUrls.add(urlHash)) {
					List<Integer> ids = hashToIds.get(contentHash);
					if (ids == null) {
						Integer prevId = hashToSingleId.get(contentHash);
						if (prevId == null) {
							hashToSingleId.put(contentHash, id);
						} else {
							ids = new ArrayList<Integer>(2);
							ids.add(prevId);
							ids.add(id);
							hashToIds.put(contentHash, ids);
							hashToSingleId.remove(contentHash);
						}
					} else {
						ids.add(id);
					}
				}

				if ((++numRead % 100000) == 0)
					System.err.println("processed " + numRead + " websites");
			}
		} finally {
			c.close();
		}

		Set<Integer> duplicateIds = new HashSet<Integer>();
		for (List<Integer> ids : hashToIds.values()) {
			if (ids.size() >= minInst)
				duplicateIds.addAll(ids);
		}
		return duplicateIds;
	}

	private static final int maxBatchSize = 10000;

	private static void removeWebsites(Set<Integer> ids) throws SQLException {
		Connection c = TwitterDao.getConnection();
		try {
			PreparedStatement st = c.prepareStatement("delete from WEBSITE_FILTERED where ID = ?");
			int batchSize = 0;
			for (Integer id : ids) {
				st.setInt(1, id);
				st.addBatch();
				if (++batchSize > maxBatchSize) {
					st.executeBatch();
					batchSize = 0;
				}
			}
			if (batchSize > 0)
				st.executeBatch();
		} finally {
			c.close();
		}
	}

	public static void main(String[] args) throws SQLException {
		int minInst = 10;
		if (args.length > 0)
			minInst = Integer.parseInt(args[0]);

		Set<Integer> duplicateIds = findDuplicateContent(minInst);
		System.err.println("found " + duplicateIds.size() + " duplicates, removing...");
		removeWebsites(duplicateIds);
		System.err.println("done");
	}

}
