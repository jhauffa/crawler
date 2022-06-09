package edu.tum.cs.crawling.twitter.dataset;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.tum.cs.crawling.twitter.server.TwitterDao;

public class BuildUserIdList {

	private static void saveUserIds(Iterable<Long> idCollection, String fileName) throws IOException {
		Writer writer = new BufferedWriter(new FileWriter(fileName));
		try {
			for (Long value : idCollection)
				writer.write(value.toString() + "\n");
		} finally {
			writer.close();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("usage: " + BuildUserIdList.class.getSimpleName() + " baseName [numUsers]");
			return;
		}

		int numUsers = -1;
		if (args.length > 1)
			numUsers = Integer.parseInt(args[1]);

		List<Long> userIds = new ArrayList<Long>();
		String sql = "SELECT id FROM user WHERE ignored = 0 AND secured = 0 AND crawling_failed = 0 ORDER BY " +
				"first_crawled_at ASC";
		if (numUsers >= 0)
			sql += " LIMIT " + numUsers;
		Connection c = TwitterDao.getConnection();
		try {
			Statement s = c.createStatement();
			ResultSet rs = s.executeQuery(sql);
			while (rs.next())
				userIds.add(rs.getLong("id"));
		} finally {
			c.close();
		}

		saveUserIds(userIds, args[0] + ".list");
		System.out.println("done");
	}

}
