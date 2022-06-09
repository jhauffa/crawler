package edu.tum.cs.crawling.facebook.dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class DumpNetlist {

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("usage: " + DumpNetlist.class.getSimpleName() +
					" host database user password crawled_only\n\tcrawled_only\ttrue/false");
			return;
		}
		boolean crawledOnly = Boolean.parseBoolean(args[4]);

		// connect to database
		String url = "jdbc:mysql://" + args[0] + "/" + args[1];
		Connection conn = DriverManager.getConnection(url, args[2], args[3]);
		try {
			Statement s = conn.createStatement();

			// retrieve IDs of fully crawled user profiles
			Set<Integer> userIds = new HashSet<Integer>();
			ResultSet rs = s.executeQuery("select personId from persondetails");
			try {
				while (rs.next())
					userIds.add(rs.getInt(1));
			} finally {
				rs.close();
			}

			// retrieve and output outgoing edges
			PreparedStatement ps = conn.prepareStatement("select friendId from friendswith where personId = ?");
			for (Integer userId : userIds) {
				ps.setInt(1, userId);
				rs = ps.executeQuery();
				try {
					while (rs.next()) {
						Integer friendId = rs.getInt(1);
						if (!crawledOnly || userIds.contains(friendId))
							System.out.println(userId + "\t" + friendId);
					}
				} finally {
					rs.close();
				}
			}
		} finally {
			conn.close();
		}
	}

}
