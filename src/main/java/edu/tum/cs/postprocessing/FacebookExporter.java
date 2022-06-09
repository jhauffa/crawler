package edu.tum.cs.postprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.tum.cs.crawling.facebook.server.FacebookDao;

public class FacebookExporter {

	private static final Logger logger = Logger.getLogger(FacebookExporter.class.getName());

	private static Set<Integer> getCoreUserIds(Connection conn) throws SQLException {
		Set<Integer> ids = new HashSet<Integer>();
		PreparedStatement ps = conn.prepareStatement("select personId from persondetails");
		try {
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next())
					ids.add(rs.getInt(1));
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return ids;
	}

	private static SocialMediaUser getUser(Connection conn, int userId, Map<Integer, SocialMediaUser> usersById)
			throws SQLException {
		SocialMediaUser user = usersById.get(userId);
		if (user == null) {
			PreparedStatement ps = conn.prepareStatement("select person.personFbId, person.name, " +
					"persondetails.numfriends from person left join persondetails " +
					"on person.personId = persondetails.personId where person.personId = ?");
			String fbId, name;
			int numFriends;
			boolean fullyCrawled;
			try {
				ps.setInt(1, userId);
				ResultSet rs = ps.executeQuery();
				try {
					rs.next();
					fbId = rs.getString(1);
					name = rs.getString(2);
					numFriends = rs.getInt(3);
					fullyCrawled = !rs.wasNull();
				} finally {
					rs.close();
				}
			} finally {
				ps.close();
			}

			user = new SocialMediaUser(fbId, name, fullyCrawled, numFriends);
			usersById.put(userId, user);
		}
		return user;
	}

	private static Collection<SocialMediaUser> getMentionedUsers(Connection conn, int postId,
			Map<Integer, SocialMediaUser> usersById) throws SQLException {
		List<SocialMediaUser> users = new ArrayList<SocialMediaUser>();
		PreparedStatement ps = conn.prepareStatement("select personId from mentionedperson where postId = ?");
		try {
			ps.setInt(1, postId);
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next())
					users.add(getUser(conn, rs.getInt(1), usersById));
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return users;
	}

	private static Collection<SocialMediaUser> getWithUsers(Connection conn, int postId,
			Map<Integer, SocialMediaUser> usersById) throws SQLException {
		List<SocialMediaUser> users = new ArrayList<SocialMediaUser>();
		PreparedStatement ps = conn.prepareStatement("select personId from withperson where postId = ?");
		try {
			ps.setInt(1, postId);
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next())
					users.add(getUser(conn, rs.getInt(1), usersById));
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return users;
	}

	private static int getCommentParentPostId(Connection conn, int commentId) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("select parentId from comment where commentId = ?");
		try {
			ps.setInt(1, commentId);
			ResultSet rs = ps.executeQuery();
			try {
				rs.next();
				return rs.getInt(1);
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
	}

	private static class EmbeddedObject {
		public final int id, type, ownerId;

		public EmbeddedObject(int id, int type, int ownerId) {
			this.id = id;
			this.type = type;
			this.ownerId = ownerId;
		}
	}

	private static EmbeddedObject getEmbeddedSharedObject(Connection conn, int postId, int postType)
			throws SQLException {
		EmbeddedObject obj = null;
		String sql = "select o.objectId, o.type, o.personId from embeddableobject as o, ";
		if (postType == 5)
			sql += "embeddablepost";
		else
			sql += "normalpost";
		sql += " as p, post as q where o.objectId = p.embeddedId and p.postId = ? and q.postId = p.postId and " +
			"o.personId <> q.personId";
		PreparedStatement ps = conn.prepareStatement(sql);
		try {
			ps.setInt(1, postId);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next())
					obj = new EmbeddedObject(rs.getInt(1), rs.getInt(2), rs.getInt(3));
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return obj;
	}

	private static Integer findSpecificPostByObject(Connection conn, EmbeddedObject obj, String postType)
			throws SQLException {
		PreparedStatement ps = conn.prepareStatement("select p.postId from " + postType + "post as p, post as q " +
				"where p.postId = q.postId and p.embeddedId = ? and q.personId = ?");
		try {
			ps.setInt(1, obj.id);
			ps.setInt(2, obj.ownerId);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next())
					return rs.getInt(1);
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return null;
	}

	private static Integer findPostByObject(Connection conn, EmbeddedObject obj) throws SQLException {
		Integer postId = findSpecificPostByObject(conn, obj, "normal");
		if (postId == null)
			postId = findSpecificPostByObject(conn, obj, "embeddable");
		return postId;
	}

	private static Integer getEmbeddedPostId(Connection conn, int objectId) throws SQLException {
		Integer embeddedPostId = null;
		PreparedStatement ps = conn.prepareStatement("select postId from embeddablepost where objectId = ?");
		try {
			ps.setInt(1, objectId);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next())
					embeddedPostId = rs.getInt(1);
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
		return embeddedPostId;
	}

	private static final Pattern emojiPattern = Pattern.compile("\u201e(.*?)\u201c-Emoticon");
	private static final Map<String, String> emojiMap;
	static {
		emojiMap = new HashMap<String, String>();
		emojiMap.put("smile", "\ud83d\ude42");
		emojiMap.put("frown", "\ud83d\ude41");
		emojiMap.put("tongue", "\ud83d\ude1b");
		emojiMap.put("grin", "\ud83d\ude03");
		emojiMap.put("gasp", "\ud83d\ude2e");
		emojiMap.put("wink", "\ud83d\ude09");
		emojiMap.put("glasses", "\ud83e\udd13");
		emojiMap.put("sunglasses", "\ud83d\ude0e");
		emojiMap.put("grumpy", "\ud83d\ude20");
		emojiMap.put("unsure", "\ud83d\ude15");
		emojiMap.put("cry", "\ud83d\ude22");
		emojiMap.put("devil", "\ud83d\ude08");
		emojiMap.put("angel", "\ud83d\ude07");
		emojiMap.put("kiss", "\ud83d\ude18");
		emojiMap.put("kiki", "\ud83d\ude0a");
		emojiMap.put("squint", "\ud83d\ude11");
		emojiMap.put("confused", "\ud83d\ude33");
		emojiMap.put("upset", "\ud83d\ude21");
		emojiMap.put("heart", "\u2764");
		emojiMap.put("robot", "\ud83e\udd16");
		emojiMap.put("penguin", "\ud83d\udc27");
		emojiMap.put("shark", "\ud83e\udd88");
		emojiMap.put("like", "\ud83d\udc4d");
		emojiMap.put("poop", "\ud83d\udca9");
	}

	private static String convertEmojiRepresentations(String text) {
		// map the textual emoji representations used by Facebook to Unicode surrogate pairs
		StringBuilder sb = new StringBuilder();
		Matcher m = emojiPattern.matcher(text);
		int textStart = 0;
		while (m.find()) {
			sb.append(text.substring(textStart, m.start()));
			String replacement = emojiMap.get(m.group(1));
			if (replacement == null)
				replacement = "fbemo" + m.group(1);
			sb.append(replacement);
			textStart = m.end();
		}
		sb.append(text.substring(textStart));
		return sb.toString();
	}

	private static int numReparentedComments = 0;
	private static int numOrphanedComments = 0;

	private static void importWallPosts(Connection conn, int wallId, Map<Integer, SocialMediaUser> usersById,
			MessageAggregator aggregator, Map<SocialMediaMessage, Integer> messageParent,
			Map<SocialMediaMessage, Integer> messageOrigin) throws SQLException {
		Map<Integer, SocialMediaMessage> messagesById = new HashMap<Integer, SocialMediaMessage>();
		Map<Integer, List<Integer>> messageIdsByType = new HashMap<Integer, List<Integer>>();

		PreparedStatement ps = conn.prepareStatement("select postId, type, personId, date, text from post " +
				"where wallId = ?");
		try {
			ps.setInt(1, wallId);
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next()) {
					int id = rs.getInt(1);
					int type = rs.getInt(2);
					int senderId = rs.getInt(3);
					Date date = null;
					Timestamp ts = rs.getTimestamp(4);
					if (ts != null)
						date = new Date(ts.getTime());
					String text = rs.getString(5);
					text = convertEmojiRepresentations(text);

					SocialMediaUser sender = getUser(conn, senderId, usersById);
					/* Recipients are assigned according to awareness, i.e. anyone who gets notified about a post is a
					 * recipient: If A posts on B's wall, B is a recipient. If B is mentioned in A's message, B is a
					 * recipient. */
					Set<SocialMediaUser> recipients = new HashSet<SocialMediaUser>();
					if (senderId != wallId)
						recipients.add(getUser(conn, wallId, usersById));
					recipients.addAll(getMentionedUsers(conn, id, usersById));
					recipients.addAll(getWithUsers(conn, id, usersById));

					// not every Facebook post has a native ID, so always use the synthetic postId instead
					SocialMediaMessage message = new SocialMediaMessage(Integer.toString(id), date, sender, recipients,
							text);
					messagesById.put(id, message);
					List<Integer> messageIds = messageIdsByType.get(type);
					if (messageIds == null) {
						messageIds = new ArrayList<Integer>();
						messageIdsByType.put(type, messageIds);
					}
					messageIds.add(id);
				}
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}

		// remove names of mentioned users from text body
		PreparedStatement psMentionedUsers = conn.prepareStatement("select p.name from person as p " +
				"inner join mentionedperson as m on p.personId = m.personId where m.postId = ?");
		try {
			for (Map.Entry<Integer, SocialMediaMessage> e : messagesById.entrySet()) {
				String postText = e.getValue().getContent();
				psMentionedUsers.setInt(1, e.getKey());
				ResultSet rs = psMentionedUsers.executeQuery();
				try {
					while (rs.next()) {
						String userName = rs.getString(1);
						postText = postText.replace(userName, "");
					}
				} finally {
					rs.close();
				}
				if (postText.length() < e.getValue().getContent().length()) {
					postText = postText.trim();
					e.getValue().setContent(postText);
				}
			}
		} finally {
			psMentionedUsers.close();
		}

		Set<Integer> protectedIds = new HashSet<Integer>();
		for (Map.Entry<Integer, List<Integer>> e : messageIdsByType.entrySet()) {
			for (Integer id : e.getValue()) {
				SocialMediaMessage message = messagesById.get(id);
				Set<SocialMediaUser> recipients = message.getRecipients();
				if (e.getKey() == 1) {
					/* For comments, the group of aware users is harder to define. The original poster and
					 * all subsequent commenters are notified, but including the latter group would make the number of
					 * recipients dependent on the attention a post receives, and particularly in the case of posts with
					 * a large comment volume it is unlikely that every commenter has read all previous comments. Since
					 * we can be sure that the comment relates to the original post, but cannot reliably tell if the
					 * comment is a reaction to one of the earlier comments, we only consider the author of the original
					 * post as a recipient. */
					int postId = getCommentParentPostId(conn, id);
					protectedIds.add(postId);
					messageParent.put(message, postId);
					// assumes that parent post is already in messagesById
					recipients.add(messagesById.get(postId).getSender());
				} else if ((e.getKey() == 2) || (e.getKey() == 5)) {
					EmbeddedObject obj = getEmbeddedSharedObject(conn, id, e.getKey());
					if (obj != null) {
						Integer originalPostId;
						if (obj.type == 5)
							originalPostId = getEmbeddedPostId(conn, obj.id);
						else
							originalPostId = findPostByObject(conn, obj);
						messageOrigin.put(message, originalPostId);
						if (originalPostId != null)
							protectedIds.add(originalPostId);
						protectedIds.add(id);
					}
				}
			}
		}

		// aggregate messages, make sure to keep parent posts of comments even if they are empty
		Map<SocialMediaMessage, Set<Integer>> candidateParents = new HashMap<SocialMediaMessage, Set<Integer>>();
		for (Map.Entry<Integer, SocialMediaMessage> e : messagesById.entrySet()) {
			SocialMediaMessage message = e.getValue();
			if (message.getDate() != null) {	// ignore posts without timestamp
				SocialMediaMessage aggMessage = aggregator.addMessage(message, protectedIds.contains(e.getKey()));

				if (aggMessage != message) {
					Integer aggParent = messageParent.get(aggMessage);
					Integer parent = messageParent.get(message);
					if ((aggParent != null) && (parent != null) && !aggParent.equals(parent)) {
						// Message is a duplicate of aggMessage, and both messages have different parents. This can
						// happen when a photo album is updated: Each update creates a wall post that contains all past
						// and future comments on that album. For each comment, keep a list of possible parents, and
						// heuristically assign a canonical parent later.
						Set<Integer> candidates = candidateParents.get(aggMessage);
						if (candidates == null) {
							candidates = new HashSet<Integer>();
							candidateParents.put(aggMessage, candidates);
						}
						candidates.add(aggParent);
						candidates.add(parent);
					}
				}
			}
		}

		// assign canonical parents to comments on photo album updates
		for (Map.Entry<SocialMediaMessage, Set<Integer>> e : candidateParents.entrySet()) {
			long commentTime = e.getKey().getDate().getTime();
			Integer minDistParent = null;
			long minDist = Long.MAX_VALUE;
			for (Integer curParent : e.getValue()) {
				long dist = commentTime - aggregator.getMessageById(curParent.toString()).getDate().getTime();
				if ((dist >= 0) && (dist < minDist)) {
					minDist = dist;
					minDistParent = curParent;
				}
			}
			if (minDistParent == null) {
				logger.warning("timestamp of comment " + e.getKey().getId() + " precedes all candidate parents");
				messageParent.remove(e.getKey());
				numOrphanedComments++;
			} else {
				messageParent.put(e.getKey(), minDistParent);
				numReparentedComments++;
			}
		}
		// At this point, some of the candidate parents may be empty and no longer have any children. This is somewhat
		// ugly, but is not an issue for the influence experiments, where empty messages are ignored, nor for the HMM
		// experiments, where reply chains of length < 2 are ignored.
	}

	private static void importRelationships(Connection conn, int userId, Map<Integer, SocialMediaUser> usersById)
			throws SQLException {
		SocialMediaUser user = usersById.get(userId);
		PreparedStatement ps = conn.prepareStatement("select friendId from friendswith where personId = ?");
		try {
			ps.setInt(1, userId);
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next()) {
					SocialMediaUser friend = usersById.get(rs.getInt(1));
					if (friend != null) {
						user.getFriends().add(friend);
						friend.getFriends().add(user);	// ensure symmetry
					}
				}
			} finally {
				rs.close();
			}
		} finally {
			ps.close();
		}
	}

	private static void importFacebookData(Connection conn, Map<Integer, SocialMediaUser> usersById,
			MessageAggregator aggregator) throws SQLException {
		Set<Integer> coreUserIds = getCoreUserIds(conn);

		Map<SocialMediaMessage, Integer> messageParent = new HashMap<SocialMediaMessage, Integer>();
		Map<SocialMediaMessage, Integer> messageOrigin = new HashMap<SocialMediaMessage, Integer>();
		int n = 0;
		for (Integer userId : coreUserIds) {
			importWallPosts(conn, userId, usersById, aggregator, messageParent, messageOrigin);
			if ((++n % 100) == 0)
				System.err.println("processed " + n + " of " + coreUserIds.size() + " core users");
		}
		// link comments to parent (only first level comments have been crawled)
		for (Map.Entry<SocialMediaMessage, Integer> e : messageParent.entrySet())
			e.getKey().updateParent(aggregator.getMessageById(e.getValue().toString()));
		// link shared posts to original post if present in the dataset
		for (Map.Entry<SocialMediaMessage, Integer> e : messageOrigin.entrySet()) {
			if (e.getValue() != null)
				e.getKey().updateOrigin(aggregator.getMessageById(e.getValue().toString()));
			else
				e.getKey().updateOrigin(null);
		}

		for (Integer userId : usersById.keySet())
			importRelationships(conn, userId, usersById);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("usage: " + FacebookExporter.class.getSimpleName() +
					" host db1 user password db2 prefix");
			return;
		}

		Map<Integer, SocialMediaUser> users = new HashMap<Integer, SocialMediaUser>();
		MessageAggregator aggregator = new MessageAggregator(false);

		FacebookDao.initialize(args[0], args[1], args[2], args[3], false);
		try {
			importFacebookData(FacebookDao.getConnection(), users, aggregator);
		} finally {
			FacebookDao.shutdown();
		}
		System.out.println("read " + users.size() + " users and " + aggregator.getMessages().size() + " messages");
		System.out.println(numReparentedComments + " reparented comments, " +
				numOrphanedComments + " orphaned comments");
		System.out.println(aggregator);

		SocialMediaDao out = new SocialMediaDao(args[0], args[4], args[2], args[3], args[5]);
		try {
			out.save(users.values(), aggregator.getMessages());
		} finally {
			out.shutdown();
		}
		System.out.println("done");
	}

}
