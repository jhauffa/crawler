package edu.tum.cs.crawling.facebook.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.DataSources;

import edu.tum.cs.crawling.facebook.entities.*;

public class FacebookDao {

	private static class EmbeddableObjectReference {
		public final int id;
		public final boolean present;
		public final boolean needsUpdate;

		public EmbeddableObjectReference(int id, boolean present, boolean needsUpdate) {
			this.id = id;
			this.present = present;
			this.needsUpdate = needsUpdate;
		}
	}

	private static final Logger logger = Logger.getLogger(FacebookDao.class.getName());
	private static final int batchSize = 1000;
	private static DataSource pooledDataSource;

	public static void initialize(String databaseIp, String databaseName, String userName, String password)
			throws ClassNotFoundException, SQLException {
		initialize(databaseIp, databaseName, userName, password, true);
	}

	public static void initialize(String databaseIp, String databaseName, String userName, String password,
			boolean createTables) throws ClassNotFoundException, SQLException {
		// connect to database
		String url = "jdbc:mysql://" + databaseIp + "/" + databaseName;
		DataSource unpooledDataSource = DataSources.unpooledDataSource(url, userName, password);
		pooledDataSource = DataSources.pooledDataSource(unpooledDataSource);

		if (createTables) {
			// create tables if they do not yet exist
			Connection conn = getConnection();
			try {
				createTables(conn);
			} finally {
				conn.close();
			}
		}
	}

	public static void shutdown() throws SQLException {
		DataSources.destroy(pooledDataSource);
	}

	private static void createTables(Connection conn) throws SQLException {
		Statement s = conn.createStatement();

		// basic objects
		// -------------

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `person` (" +
				"`personId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`personFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`personId`), UNIQUE KEY `personFbId_UNIQUE` (`personFbId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `place` (" +
				"`placeId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`placeFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`placeId`), UNIQUE KEY `placeFbId_UNIQUE` (`placeFbId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `site` (" +
				"`siteId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`siteFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`siteId`), UNIQUE KEY `siteFbId_UNIQUE` (`siteFbId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `post` (" +
				"`postId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`type` tinyint unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +
				"`wallId` int(10) unsigned NOT NULL," +
				"`index` int(10) unsigned NOT NULL," +
				"`date` datetime DEFAULT NULL," +
				"`text` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`postId`), KEY `posttopers_idx` (`personId`)," +
				"CONSTRAINT `posttopers` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `posttowall` FOREIGN KEY (`wallId`) REFERENCES `person` (`personId`))");

		// embeddable objects and master table
		// -----------------------------------

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `embeddableobject` (" +
				"`objectId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`objectFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`type` tinyint unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +	// "owner" / original poster
				"`isStub` boolean NOT NULL," +
				"PRIMARY KEY (`objectId`), UNIQUE KEY `objectFbId_UNIQUE` (`objectFbId`)," +
				"CONSTRAINT `objecttoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `video` (" +
				"`objectId` int(10) unsigned NOT NULL," +
				"`title` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`shareComment` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"CONSTRAINT `videotoobject` FOREIGN KEY (`objectId`) REFERENCES `embeddableobject` (`objectId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `link` (" +
				"`objectId` int(10) unsigned NOT NULL," +
				"`url` varchar(2083) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"CONSTRAINT `linktoobject` FOREIGN KEY (`objectId`) REFERENCES `embeddableobject` (`objectId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `photoalbum` (" +
				"`objectId` int(10) unsigned NOT NULL," +
				"`name` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`shareComment` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
				"CONSTRAINT `albumtoobject` FOREIGN KEY (`objectId`) REFERENCES `embeddableobject` (`objectId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `photo` (" +
				"`objectId` int(10) unsigned NOT NULL," +
				"`albumId` int(10) unsigned NOT NULL," +
				"`shareComment` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
				"CONSTRAINT `phototoobject` FOREIGN KEY (`objectId`) REFERENCES `embeddableobject` (`objectId`)," +
				"CONSTRAINT `phototoalbum` FOREIGN KEY (`albumId`) REFERENCES `embeddableobject` (`objectId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `embeddablepost` (" +
				"`objectId` int(10) unsigned NOT NULL," +
				"`postId` int(10) unsigned NOT NULL," +
				"`type` tinyint unsigned NOT NULL," +
				"`placeId` int(10) unsigned DEFAULT NULL," +
				"`embeddedId` int(10) unsigned DEFAULT NULL," +
				"PRIMARY KEY (`postId`)," +
				"CONSTRAINT `eptoobject` FOREIGN KEY (`objectId`) REFERENCES `embeddableobject` (`objectId`)," +
				"CONSTRAINT `eptopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postId`)," +
				"CONSTRAINT `eptoplace` FOREIGN KEY (`placeId`) REFERENCES `place` (`placeId`)," +
				"CONSTRAINT `eptoembedded` FOREIGN KEY (`embeddedId`) REFERENCES `embeddableobject` (`objectId`))");

		// data associated with "post" objects
		// -----------------------------------

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `normalpost` (" +
				"`postId` int(10) unsigned NOT NULL," +
				"`type` tinyint unsigned NOT NULL," +
				"`placeId` int(10) unsigned DEFAULT NULL," +
				"`embeddedId` int(10) unsigned DEFAULT NULL," +
				"PRIMARY KEY (`postId`)," +
				"CONSTRAINT `normaltopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postId`)," +
				"CONSTRAINT `normaltoplace` FOREIGN KEY (`placeId`) REFERENCES `place` (`placeId`)," +
				"CONSTRAINT `normaltoembedded` FOREIGN KEY (`embeddedId`) REFERENCES `embeddableobject` (`objectId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `likes` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`postId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`personId`,`postId`)," +
				"KEY `liketopost_idx` (`postId`), KEY `liketoperson_idx` (`personId`)," +
				"CONSTRAINT `liketoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `liketopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postid`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `comment` (" +
				"`commentId` int(10) unsigned NOT NULL," +
				"`parentId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`commentId`), KEY `comtopost_idx` (`parentId`)," +
				"CONSTRAINT `comtoparent` FOREIGN KEY (`parentId`) REFERENCES `post` (`postid`)," +
				"CONSTRAINT `comtopost` FOREIGN KEY (`commentId`) REFERENCES `post` (`postid`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `lifeevent` (" +
				"`postId` int(10) unsigned NOT NULL," +
				"`title` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`subtitle` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL," +
				"PRIMARY KEY (`postId`)," +
				"CONSTRAINT `lifetopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postid`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `withperson` (" +
				"`postId` int(10) unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`postId`,`personId`), KEY `withpers_idx` (`personId`), KEY `withpos_idx` (`postId`)," +
				"CONSTRAINT `withtoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `withtopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `mentionedperson` (" +
				"`postId` int(10) unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`postId`,`personId`)," +
				"CONSTRAINT `mtnperstoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `mtnperstopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `mentionedsite` (" +
				"`postId` int(10) unsigned NOT NULL," +
				"`siteId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`postId`,`siteId`)," +
				"CONSTRAINT `mtnsitetosite` FOREIGN KEY (`siteId`) REFERENCES `site` (`siteId`)," +
				"CONSTRAINT `mtnsitetopost` FOREIGN KEY (`postId`) REFERENCES `post` (`postId`))");

		// data associated with "person" objects
		// -------------------------------------

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `friendswith` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`friendId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`personId`,`friendId`)," +
				"KEY `person1foreign_idx` (`personId`), KEY `person2foreign_idx` (`friendId`)," +
				"CONSTRAINT `person1foreign` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `person2foreign` FOREIGN KEY (`friendId`) REFERENCES `person` (`personId`))");

		/* Contains various kinds of personal information a user can choose to disclose, e.g. date of birth, email
		 * address, or place of residence. 'gender' and 'interestedin' can have the values 'm', 'f', 'b', or 'u'. */
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `persondetails` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`gender` char(1) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'u'," +
				"`interestedin` char(1) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'u'," +
				"`birthday` date DEFAULT NULL," +
				"`religion` varchar(45) NOT NULL," +
				"`religionFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`politics` varchar(45) NOT NULL," +
				"`politicsFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`phonenumber` varchar(45) NOT NULL," +
				"`address` varchar(256) NOT NULL," +
				"`homepage` varchar(64) NOT NULL," +
				"`email` varchar(256) NOT NULL," +
				"`relationshipstatus` varchar(45) NOT NULL," +
				"`currentresidence` varchar(45) NOT NULL," +
				"`currentresidenceFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`hometown` varchar(45) NOT NULL," +
				"`hometownFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`bio` text NOT NULL," +
				"`quotes` text NOT NULL," +
				"`numfriends` int(10) unsigned NOT NULL," +
				"`detectedlanguage` varchar(5) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"PRIMARY KEY (`personId`)," +
				"CONSTRAINT `detailstoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)) " +
				"CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `spokenlanguages` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`language` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`personId`,`language`)," +
				"CONSTRAINT `languagetoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`))");

		// Describes the role of the relationship between two persons.
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `familymember` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`familymemberId` int(10) unsigned NOT NULL," +
				"`role` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`personId`,`familymemberId`)," +
				"KEY `persontofamily_idx` (`personId`), KEY `persontofamily2_idx` (`familymemberId`)," +
				"CONSTRAINT `persontofamily` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `persontofamily2` FOREIGN KEY (`familymemberId`) REFERENCES `person` (`personId`))");

		s.executeUpdate("CREATE TABLE IF NOT EXISTS `educationitem` (" +
				"`educationItemId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`personId` int(10) unsigned NOT NULL," +
				"`index` int(10) unsigned NOT NULL," +
				"`name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`educationFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`timeperiod` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`type` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`field` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`educationItemId`), KEY `educationtoperson_idx` (`personId`)," +
				"CONSTRAINT `educationtoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `educationclass` (" +
				"`educationclassId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`parentId` int(10) unsigned NOT NULL," +
				"`index` int(10) unsigned NOT NULL," +
				"`name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`educationclassId`), KEY `classtoeducation_idx` (`parentId`)," +
				"CONSTRAINT `classtoeducation` FOREIGN KEY (`parentId`) " +
					"REFERENCES `educationitem` (`educationItemId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `educationwithperson` (" +
				"`educationclassId` int(10) unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`educationclassId`,`personId`), KEY `withpersoneductoperson_idx` (`personId`)," +
				"CONSTRAINT `withpersoneductoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `withpersontoeduc` FOREIGN KEY (`educationclassId`) " +
					"REFERENCES `educationclass` (`educationclassId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `workitem` (" +
				"`workitemid` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`personId` int(10) unsigned NOT NULL," +
				"`index` int(10) unsigned NOT NULL," +
				"`name` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`workFbId` varchar(64) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL," +
				"`title` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`timeperiod` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`place` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`workitemid`), KEY `worktoperson_idx` (`personId`)," +
				"CONSTRAINT `worktoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `workproject` (" +
				"`workprojectId` int(10) unsigned NOT NULL AUTO_INCREMENT," +
				"`parentId` int(10) unsigned NOT NULL," +
				"`index` int(10) unsigned NOT NULL," +
				"`name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`timeperiod` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"`description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
				"PRIMARY KEY (`workprojectId`), KEY `projecttowork_idx` (`parentId`)," +
				"CONSTRAINT `projecttowork` FOREIGN KEY (`parentId`) REFERENCES `workitem` (`workitemid`))");
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `workwithperson` (" +
				"`workprojectId` int(10) unsigned NOT NULL," +
				"`personId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`workprojectId`,`personId`), KEY `workwithtoperson_idx` (`personId`)," +
				"CONSTRAINT `secondaryforwork` FOREIGN KEY (`workprojectId`) " +
					"REFERENCES `workproject` (`workprojectId`)," +
				"CONSTRAINT `workwithtoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`))");

		// Contains likes that are not associated with posts, but pages of e.g. bands or movies.
		s.executeUpdate("CREATE TABLE IF NOT EXISTS `sitelikes` (" +
				"`personId` int(10) unsigned NOT NULL," +
				"`siteId` int(10) unsigned NOT NULL," +
				"PRIMARY KEY (`personId`,`siteId`)," +
				"CONSTRAINT `sitetoperson` FOREIGN KEY (`personId`) REFERENCES `person` (`personId`)," +
				"CONSTRAINT `sitetosite` FOREIGN KEY (`siteId`) REFERENCES `site` (`siteId`))");
	}

	public static Connection getConnection() throws SQLException {
		return pooledDataSource.getConnection();
	}

	public static void saveUserProfile(UserProfile profile) throws Exception {
		String userId = profile.getUser().getId();
		String userName = profile.getUser().getName();

		assessCompleteness(profile);

		Connection conn = getConnection();
		try {
			conn.setAutoCommit(false);
			savePosts(conn, profile.getPosts(), userId, userName);
			saveFriends(conn, profile.getFriends(), userId, userName);
			saveSiteLikes(conn, profile.getUser(), profile.getLikedsites());
			savePersonDetails(conn, profile.getDetails(), profile.getNumFriends(), profile.getLanguage(), userId,
					userName);
			conn.commit();
		} catch (Exception ex) {
			conn.rollback();
			throw ex;
		} finally {
			conn.close();
		}
	}

	/** A simple diagnostic to make sure that the crawler is still able to extract all parts of the user profile. */
	private static void assessCompleteness(UserProfile profile) {
		boolean hasPosts = !profile.getPosts().isEmpty();
		boolean hasSiteLikes = !profile.getLikedsites().isEmpty();
		float detailsCompleteness = profile.getDetails().assessCompleteness();

		int expectedFriends = profile.getNumFriends();
		float foundFriends = 1.0f;
		if (expectedFriends > 0)
			foundFriends = (float) profile.getFriends().size() / expectedFriends;

		// TODO: A common problem is that Facebook changes the site layout and thus the DOM structure for one or more of
		//	the entities we're trying to extract, and this is not directly visible (especially in the case of optional
		//	elements, where the extractor cannot warn about their absence). A rigorous solution would be to create a
		//	reference FB profile that contains all elements we're extracting and has them set to known values. Extracted
		//	profiles are not saved to the database immediately, but stored in a buffer. After n profiles have been
		//	extracted, the reference profile is crawled, and the extraction result is compared to the known values. If
		//	everything matches, the buffered profiles are saved, otherwise the profiles are discarded, a warning is
		//	printed, and crawling stops. Trading accuracy for simplicity, one could keep counters for the different
		//	entities (posts, likes, details, ...). A counter would be incremented each time a profile did not contain
		//	the corresponding entity, and reset to 0 each time the entity was present. Any time a counter exceeds a
		//	fixed threshold, a warning is printed and crawling stops. The current implementation can tell you if there
		//	have been major changes, but requires manual monitoring of the server logs.
		logger.info("profile '" + profile.getUser().getId() + "': posts=" + hasPosts + ", site likes=" + hasSiteLikes +
				", friends=" + foundFriends + " (" + expectedFriends + "), details=" + detailsCompleteness);
	}

	private static Timestamp convertDate(Date date) {
		if(date == null)
			return null;
		return new Timestamp(date.getTime());
	}

	private static String truncateString(String s, int maxLength, String field) {
		if(s.length() > maxLength) {
			logger.warning("truncating value of '" + field + "' from " + s.length() + " to " + maxLength + " chars");
			s = s.substring(0, maxLength);
		}
		return s;
	}

	/**
	 * Retrieves the PersonId of the specified person from the database. If the person is not yet in the database, it
	 * is saved first.
	 * @param userid Facebook ID string of the person
	 * @param name full name of the person
	 * @return personid database ID of the specified person
	 * @throws SQLException
	 */
	private static int getPersonId(Connection conn, String userFbId, String name) throws SQLException {
		PreparedStatement getPersonId = conn.prepareStatement("select personId from person where personFbId=?");
		try {
			getPersonId.setString(1, userFbId);
			ResultSet rs = getPersonId.executeQuery();
			try {
				if(rs.next())
					return rs.getInt("personId");
				return insertPerson(conn, userFbId, name);	// user is not yet in database, insert
			} finally {
				rs.close();
			}
		} finally {
			getPersonId.close();
		}
	}

	private static int insertPerson(Connection conn, String userFbId, String name) throws SQLException {
		PreparedStatement insertPerson = conn.prepareStatement("insert into person(personFbId, name) values(?, ?)",
				Statement.RETURN_GENERATED_KEYS);
		try {
			insertPerson.setString(1, userFbId);
			insertPerson.setString(2, truncateString(name, 512, "person.name"));
			insertPerson.executeUpdate();
			ResultSet res = insertPerson.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);	// key of inserted row, same as personId
			} finally {
				res.close();
			}
		} finally {
			insertPerson.close();
		}
	}

	private static int getPlaceId(Connection conn, Place place) throws SQLException {
		PreparedStatement getPlaceId = conn.prepareStatement("select placeId from place where placeFbId=?");
		try {
			getPlaceId.setString(1, place.getId());
			ResultSet rs = getPlaceId.executeQuery();
			try {
				if(rs.next())
					return rs.getInt("placeId");
				return insertPlace(conn, place);	// place is not yet in database, insert
			} finally {
				rs.close();
			}
		} finally {
			getPlaceId.close();
		}
	}

	private static int insertPlace(Connection conn, Place place) throws SQLException {
		PreparedStatement insertPlace = conn.prepareStatement("insert into place(placeFbId, name) values(?, ?)",
				Statement.RETURN_GENERATED_KEYS);
		try {
			insertPlace.setString(1, place.getId());
			insertPlace.setString(2, truncateString(place.getName(), 512, "place.name"));
			insertPlace.executeUpdate();
			ResultSet res = insertPlace.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);
			} finally {
				res.close();
			}
		} finally {
			insertPlace.close();
		}
	}

	private static int getSiteId(Connection conn, Site site) throws SQLException {
		PreparedStatement getSiteId = conn.prepareStatement("select siteId from site where siteFbId=?");
		try {
			getSiteId.setString(1, site.getId());
			ResultSet rs = getSiteId.executeQuery();
			try {
				if(rs.next())
					return rs.getInt("siteId");
				return insertSite(conn, site);	// site is not yet in database, insert
			} finally {
				rs.close();
			}
		} finally {
			getSiteId.close();
		}
	}

	private static int insertSite(Connection conn, Site site) throws SQLException {
		PreparedStatement insertSite = conn.prepareStatement("insert into site(siteFbId, name) values(?, ?)",
				Statement.RETURN_GENERATED_KEYS);
		try {
			insertSite.setString(1, site.getId());
			insertSite.setString(2, truncateString(site.getName(), 512, "site.name"));
			insertSite.executeUpdate();
			ResultSet res = insertSite.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);
			} finally {
				res.close();
			}
		} finally {
			insertSite.close();
		}
	}

	private static EmbeddableObjectReference getEmbeddableObjectReference(Connection conn, EmbeddableObject obj,
			Person owner, boolean isStub) throws SQLException {
		PreparedStatement getEmbeddableObjectId = conn.prepareStatement("select objectId, isStub " +
				"from embeddableobject where objectFbId=?");
		try {
			getEmbeddableObjectId.setString(1, obj.getId());
			ResultSet rs = getEmbeddableObjectId.executeQuery();
			try {
				if(rs.next()) {
					int id = rs.getInt("objectId");
					if(rs.getBoolean("isStub") && !isStub)	// replacing stub with actual object, update status flag
						return updateEmbeddableObject(conn, id);
					return new EmbeddableObjectReference(rs.getInt("objectId"), true, false);
				}
				return insertEmbeddableObject(conn, obj, owner, isStub);
			} finally {
				rs.close();
			}
		} finally {
			getEmbeddableObjectId.close();
		}
	}

	private static EmbeddableObjectReference insertEmbeddableObject(Connection conn, EmbeddableObject obj, Person owner,
			boolean isStub) throws SQLException {
		// determine object type
		int type;
		if (obj instanceof Video)
			type = 1;
		else if (obj instanceof Link)
			type = 2;
		else if (obj instanceof PhotoAlbum)
			type = 3;
		else if (obj instanceof Photo)
			type = 4;
		else if (obj instanceof EmbeddableNormalPost)
			type = 5;
		else
			throw new RuntimeException("unknown embeddable object type " + obj.getClass().getName());

		// object is not yet in database, insert
		int personId = getPersonId(conn, owner.getId(), owner.getName());

		PreparedStatement insertEmbeddableObject = conn.prepareStatement("insert into embeddableobject(objectFbId, " +
				"type, personId, isStub) values(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		try {
			insertEmbeddableObject.setString(1, obj.getId());
			insertEmbeddableObject.setInt(2, type);
			insertEmbeddableObject.setInt(3, personId);
			insertEmbeddableObject.setBoolean(4, isStub);
			insertEmbeddableObject.executeUpdate();
			ResultSet res = insertEmbeddableObject.getGeneratedKeys();
			try {
				res.next();
				return new EmbeddableObjectReference(res.getInt(1), false, true);
			} finally {
				res.close();
			}
		} finally {
			insertEmbeddableObject.close();
		}
	}

	private static EmbeddableObjectReference updateEmbeddableObject(Connection conn, int id) throws SQLException {
		PreparedStatement updateEmbeddableObject = conn.prepareStatement("update embeddableobject set isStub=0 where " +
				"objectId=?");
		try {
			updateEmbeddableObject.setInt(1, id);
			updateEmbeddableObject.executeUpdate();
			return new EmbeddableObjectReference(id, true, true);
		} finally {
			updateEmbeddableObject.close();
		}
	}

	private static int saveVideo(Connection conn, Video video, Person owner, boolean isStub) throws SQLException {
		EmbeddableObjectReference ref = getEmbeddableObjectReference(conn, video, owner, isStub);
		if (ref.needsUpdate) {
			PreparedStatement st;
			if (ref.present)
				st = conn.prepareStatement("update video set title=?, shareComment=? where objectId=?");
			else
				st = conn.prepareStatement("insert into video(title, shareComment, objectId) values(?, ?, ?)");
			try {
				st.setString(1, truncateString(video.getTitle(), 200, "video.title"));
				st.setString(2, video.getShareComment());
				st.setInt(3, ref.id);
				st.executeUpdate();
			} finally {
				st.close();
			}
		}
		return ref.id;
	}

	private static int saveLink(Connection conn, Link link, Person owner, boolean isStub) throws SQLException {
		EmbeddableObjectReference ref = getEmbeddableObjectReference(conn, link, owner, isStub);
		if (ref.needsUpdate) {
			PreparedStatement st;
			if (ref.present)
				st = conn.prepareStatement("update link set url=? where objectId=?");
			else
				st = conn.prepareStatement("insert into link(url, objectId) values(?, ?)");
			try {
				st.setString(1, truncateString(link.getUrl(), 2083, "link.url"));
				st.setInt(2, ref.id);
				st.executeUpdate();
			} finally {
				st.close();
			}
		}
		return ref.id;
	}

	private static int savePhotoAlbum(Connection conn, PhotoAlbum album, Person owner, boolean isStub)
			throws SQLException {
		EmbeddableObjectReference ref = getEmbeddableObjectReference(conn, album, owner, isStub);
		if (ref.needsUpdate) {
			PreparedStatement st;
			if (ref.present)
				st = conn.prepareStatement("update photoalbum set name=?, shareComment=? where objectId=?");
			else
				st = conn.prepareStatement("insert into photoalbum(name, shareComment, objectId) values(?, ?, ?)");
			try {
				st.setString(1, truncateString(album.getName(), 512, "photoalbum.name"));
				st.setString(2, album.getShareComment());
				st.setInt(3, ref.id);
				st.executeUpdate();
			} finally {
				st.close();
			}
		}
		return ref.id;
	}

	private static int savePhoto(Connection conn, Photo photo, Person owner, boolean isStub) throws SQLException {
		EmbeddableObjectReference ref = getEmbeddableObjectReference(conn, photo, owner, isStub);
		if (ref.needsUpdate) {
			// insert dummy album if actual parent album not yet in DB
			int albumId = savePhotoAlbum(conn, new PhotoAlbum(photo.getAlbumId(), "", ""), owner, true);

			PreparedStatement st;
			if (ref.present)
				st = conn.prepareStatement("update photo set albumId=?, shareComment=? where objectId=?");
			else
				st = conn.prepareStatement("insert into photo(albumId, shareComment, objectId) values(?, ?, ?)");
			try {
				st.setInt(1, albumId);
				st.setString(2, photo.getShareComment());
				st.setInt(3, ref.id);
				st.executeUpdate();
			} finally {
				st.close();
			}
		}
		return ref.id;
	}

	private static Integer saveEmbeddedContent(Connection conn, NormalPost post) throws SQLException {
		Integer objectId = null;
		EmbeddableObject obj = post.getEmbeddedContent();
		if (obj != null) {
			NormalPostHeader header = post.getHeader();
			Person embeddedOwner = header.getSharedVia();
			boolean embeddedIsStub = true;
			if (embeddedOwner == null) {
				embeddedOwner = header.getSender();
				embeddedIsStub = false;
			}
			if (obj instanceof Video)
				objectId = saveVideo(conn, (Video) obj, embeddedOwner, embeddedIsStub);
			else if (obj instanceof Link)
				objectId = saveLink(conn, (Link) obj, embeddedOwner, embeddedIsStub);
			else if (obj instanceof PhotoAlbum)
				objectId = savePhotoAlbum(conn, (PhotoAlbum) obj, embeddedOwner, embeddedIsStub);
			else if (obj instanceof Photo)
				objectId = savePhoto(conn, (Photo) obj, embeddedOwner, embeddedIsStub);
			else if (obj instanceof EmbeddableNormalPost)
				objectId = saveEmbeddableNormalPost(conn, (EmbeddableNormalPost) obj, 0, embeddedOwner,
						embeddedOwner.getId(), embeddedOwner.getName(), embeddedIsStub)[0];
			else
				throw new RuntimeException("unknown embedded content type " + obj);
		}
		return objectId;
	}

	private static int[] saveEmbeddableNormalPost(Connection conn, EmbeddableNormalPost post, int postIdx, Person owner,
			String curUserId, String curUserName, boolean isStub) throws SQLException {
		int postId = -1;

		EmbeddableObjectReference ref = getEmbeddableObjectReference(conn, post, owner, isStub);
		boolean savePost = ref.needsUpdate;
		if (ref.present) {
			if (!ref.needsUpdate && !isStub) {
				// special case: we're seeing a complete post that has already been seen as a complete post on a
				// different wall; this can happen when a user posts to another user's wall; in this case, we save the
				// post once for each wall it appears on, but use the same objectId for each instance
				savePost = true;
			} else {
				// the common case: either the full post or a stub is already in the DB -> get the post ID
				PreparedStatement getEmbeddablePostId = conn.prepareStatement("select postId from embeddablepost " +
						"where objectId=?");
				try {
					getEmbeddablePostId.setInt(1, ref.id);
					ResultSet rs = getEmbeddablePostId.executeQuery();
					try {
						rs.next();
						postId = rs.getInt(1);
					} finally {
						rs.close();
					}
				} finally {
					getEmbeddablePostId.close();
				}
			}
		}

		if (savePost) {
			int newPostId = insertPost(conn, post, postId, postIdx, curUserId, curUserName);

			NormalPostHeader header = post.getHeader();
			Integer placeId = null;
			Place place = header.getPlace();
			if (place != null)
				placeId = getPlaceId(conn, place);

			Integer objectId = saveEmbeddedContent(conn, post);

			PreparedStatement st;
			if (postId != -1)
				st = conn.prepareStatement("update embeddablepost set postId=?, type=?, placeId=?, embeddedId=? " +
						"where objectId=?");
			else
				st = conn.prepareStatement("insert into embeddablepost(postId, type, placeId, embeddedId, objectId) " +
						"values(?, ?, ?, ?, ?)");
			try {
				postId = newPostId;
				st.setInt(1, postId);
				st.setInt(2, header.getType().ordinal());
				st.setObject(3, placeId, java.sql.Types.INTEGER);
				st.setObject(4, objectId, java.sql.Types.INTEGER);
				st.setInt(5, ref.id);
				st.executeUpdate();
			} finally {
				st.close();
			}
		}

		return new int[] { ref.id, postId };
	}

	/**
	 * Saves the specified site likes in the database.
	 */
	private static void saveSiteLikes(Connection conn, Person p, Set<Site> sitelikes) throws SQLException {
		int personId = getPersonId(conn, p.getId(), p.getName());

		List<Integer> siteIds = new ArrayList<Integer>(sitelikes.size());
		for (Site site : sitelikes)
			siteIds.add(getSiteId(conn, site));

		PreparedStatement insertSiteLikes = conn.prepareStatement("insert into sitelikes(personId, siteId) " +
				"values(?, ?)");
		try {
			// from http://viralpatel.net/blogs/batch-insert-in-java-jdbc/
			int count = 0;
			for(Integer siteId : siteIds) {
				insertSiteLikes.setInt(1, personId);
				insertSiteLikes.setInt(2, siteId);
				insertSiteLikes.addBatch();

				if(++count % batchSize == 0) {
					insertSiteLikes.executeBatch();
				}
			}
			insertSiteLikes.executeBatch();
		} finally {
			insertSiteLikes.close();
		}
	}

	/**
	 * Saves a list of posts, including Video, LifeEvent, and similar child elements in the database.
	 */
	private static void savePosts(Connection conn, List<Post> postList, String userid, String username)
			throws SQLException {
		int postIdx = 0;
		for(Post element : postList) {
			// Insertion and possible update of embeddable posts is handled by saveEmbeddableNormalPost. We assume that
			// an embedded post does not have any comments, likes, mentions, or associated persons, so if it is replaced
			// with the actual post later on, no update logic is needed for the respective child tables.
			int postid = -1;
			if (!(element instanceof EmbeddableNormalPost))	// common fields of all post types
				postid = insertPost(conn, element, -1, postIdx++, userid, username);

			// specific fields of post type
			NormalPostHeader header = null;
			Set<Person> mentionedPersons = null;
			Set<Site> mentionedSites = null;
			if(element instanceof EmbeddableNormalPost) {
				header = ((EmbeddableNormalPost) element).getHeader();
				mentionedPersons = ((EmbeddableNormalPost) element).getMentionedPersons();
				mentionedSites = ((EmbeddableNormalPost) element).getMentionedSites();
				postid = saveEmbeddableNormalPost(conn, (EmbeddableNormalPost) element, postIdx++, header.getSender(),
						userid, username, false)[1];
			} else if(element instanceof NormalPost) {
				header = ((NormalPost) element).getHeader();
				mentionedPersons = ((NormalPost) element).getMentionedPersons();
				mentionedSites = ((NormalPost) element).getMentionedSites();

				Integer placeId = null;
				Place place = header.getPlace();
				if (place != null)
					placeId = getPlaceId(conn, place);

				Integer objectId = saveEmbeddedContent(conn, (NormalPost) element);

				PreparedStatement insertNormalPost = conn.prepareStatement("insert into normalpost(postId, type, " +
						"placeId, embeddedId) values(?, ?, ?, ?)");
				try {
					insertNormalPost.setInt(1, postid);
					insertNormalPost.setInt(2, header.getType().ordinal());
					insertNormalPost.setObject(3, placeId, java.sql.Types.INTEGER);
					insertNormalPost.setObject(4, objectId, java.sql.Types.INTEGER);
					insertNormalPost.executeUpdate();
				} finally {
					insertNormalPost.close();
				}
			} else if(element instanceof LifeEvent) {
				PreparedStatement insertLifeEvent = conn.prepareStatement("insert into lifeevent(postId, title, " +
						"subtitle) values(?, ?, ?)");
				try {
					insertLifeEvent.setInt(1, postid);
					insertLifeEvent.setString(2, truncateString(((LifeEvent) element).getTitle(), 512,
							"lifeevent.title"));
					insertLifeEvent.setString(3, truncateString(((LifeEvent) element).getSubtitle(), 512,
							"lifeevent.subtitle"));
					insertLifeEvent.executeUpdate();
				} finally {
					insertLifeEvent.close();
				}
			}
			// otherwise it is a SmallPost without any specific fields

			// save associated people
			Set<Person> persons;
			if(header != null) {
				persons = header.getPersons();
				if(persons != null) {
					PreparedStatement insertWithperson = conn.prepareStatement("insert into withperson(postId, " +
							"personId) values(?, ?)");
					try {
						for(Person p : persons) {
							int personid = getPersonId(conn, p.getId(), p.getName());
							insertWithperson.setInt(1, postid);
							insertWithperson.setInt(2, personid);
							insertWithperson.executeUpdate();
						}
					} finally {
						insertWithperson.close();
					}
				}
			}

			// save mentioned persons/sites
			if(mentionedPersons != null) {
				PreparedStatement insertMentionedPerson = conn.prepareStatement("insert into mentionedperson(postId, " +
						"personId) values(?, ?)");
				try {
					for(Person p : mentionedPersons) {
						int personid = getPersonId(conn, p.getId(), p.getName());
						insertMentionedPerson.setInt(1, postid);
						insertMentionedPerson.setInt(2, personid);
						insertMentionedPerson.executeUpdate();
					}
				} finally {
					insertMentionedPerson.close();
				}
			}
			if(mentionedSites != null) {
				PreparedStatement insertMentionedSite = conn.prepareStatement("insert into mentionedsite(postId, " +
						"siteId) values(?, ?)");
				try {
					for(Site s : mentionedSites) {
						int siteid = getSiteId(conn, s);
						insertMentionedSite.setInt(1, postid);
						insertMentionedSite.setInt(2, siteid);
						insertMentionedSite.executeUpdate();
					}
				} finally {
					insertMentionedSite.close();
				}
			}

			// save likes
			savePostLikes(conn, postid, element.getLikes());

			// save comments
			PreparedStatement insertComment = conn.prepareStatement("insert into comment(commentId, parentId) " +
					"values(?, ?)");
			try {
				int commentIdx = 0;
				for(Comment comm : element.getComments()) {
					// insert post
					int childpostid = insertPost(conn, comm, commentIdx++, userid, username);
					// insert comment
					insertComment.setInt(1, childpostid);
					insertComment.setInt(2, postid);
					insertComment.executeUpdate();
					// insert likes of comment
					savePostLikes(conn, childpostid, comm.getLikes());
				}
			} finally {
				insertComment.close();
			}
		}
	}

	private static void savePostLikes(Connection conn, int postId, Set<Person> likes) throws SQLException {
		PreparedStatement insertLikes = conn.prepareStatement("insert into likes(personId, postId) values(?, ?)");
		try {
			int i = 0;
			for(Person p : likes) {
				int personId = getPersonId(conn, p.getId(), p.getName());
				insertLikes.setInt(1, personId);
				insertLikes.setInt(2, postId);
				insertLikes.addBatch();

				if(++i % batchSize == 0)
					insertLikes.executeBatch();
			}
			insertLikes.executeBatch();
		} finally {
			insertLikes.close();
		}
	}

	/**
	 * Save list of friends of the specified user in the database.
	 */
	private static void saveFriends(Connection conn, Set<Person> friends, String userid, String username)
			throws SQLException {
		int personid = getPersonId(conn, userid, username);

		PreparedStatement insertFriends = conn.prepareStatement("insert into friendswith(personId, friendId) " +
				"values(?, ?)");
		try {
			int i = 0;
			for(Person p : friends) {
				// "friendship" edges on Facebook are always undirected, but we only store the edges going out from the
				// currently crawled user to save space and avoid problems trying to insert the same edge twice.
				int friendid = getPersonId(conn, p.getId(), p.getName());
				insertFriends.setInt(1, personid);
				insertFriends.setInt(2, friendid);
				insertFriends.addBatch();

				if(++i % (batchSize/2) == 0) {
					insertFriends.executeBatch();
				}
			}
			insertFriends.executeBatch();
		} finally {
			insertFriends.close();
		}
	}

	/**
	 * Save personal data such as date of birth, place of residence, or email address. If user has not yet been saved in
	 * database, insert him first.
	 */
	private static void savePersonDetails(Connection conn, PersonDetails details, int numberOfFriends, String language,
			String userid, String username) throws SQLException {
		int personid = getPersonId(conn, userid, username);

		PreparedStatement insertPersonDetails = conn.prepareStatement("insert into persondetails(personId, gender," +
				"interestedin, birthday, religion, religionFbId, politics, politicsFbId, phonenumber, address, " +
				"homepage, email, relationshipstatus, currentresidence, currentresidenceFbId, hometown, " +
				"hometownFbId, bio, quotes, numfriends, detectedlanguage) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
				"?, ?, ?, ?, ?, ?, ?, ?, ?)");
		try {
			insertPersonDetails.setInt(1,personid);
			insertPersonDetails.setString(2,String.valueOf(details.getGender()));
			insertPersonDetails.setString(3,String.valueOf(details.getInterestedin()));
			insertPersonDetails.setTimestamp(4,convertDate(details.getBirthdate()));
			insertPersonDetails.setString(5,truncateString(details.getReligion(), 45, "persondetails.religion"));
			insertPersonDetails.setString(6,details.getReligionid());
			insertPersonDetails.setString(7,truncateString(details.getPolitics(), 45, "persondetails.politics"));
			insertPersonDetails.setString(8,details.getPoliticsid());
			insertPersonDetails.setString(9,truncateString(details.getTelephonenumber(), 45,
					"persondetails.phonenumber"));
			insertPersonDetails.setString(10,truncateString(details.getAddress(), 256, "persondetails.address"));
			insertPersonDetails.setString(11,truncateString(details.getHomepage(), 64, "persondetails.homepage"));
			insertPersonDetails.setString(12,truncateString(details.getEmail(), 256, "persondetails.email"));
			insertPersonDetails.setString(13,truncateString(details.getRelationshipstatus(), 45,
					"persondetails.relationshipstatus"));
			insertPersonDetails.setString(14,truncateString(details.getCurrentresidence(), 45,
					"persondetails.currentresidence"));
			insertPersonDetails.setString(15,details.getCurrentresidenceid());
			insertPersonDetails.setString(16,truncateString(details.getHometown(), 45, "persondetails.hometown"));
			insertPersonDetails.setString(17,details.getHometownid());
			insertPersonDetails.setString(18,details.getBio());
			insertPersonDetails.setString(19,details.getQuotes());
			insertPersonDetails.setInt(20,Math.max(0, numberOfFriends));
			insertPersonDetails.setString(21,truncateString(language, 5, "persondetails.detectedlanguage"));
			insertPersonDetails.executeUpdate();
		} finally {
			insertPersonDetails.close();
		}

		// save spoken languages
		if(details.getSpokenlanguages() != null) {
			PreparedStatement insertSpokenLanguages = conn.prepareStatement("insert into spokenlanguages(personId, " +
					"language) values(?, ?)");
			try {
				for(String s : details.getSpokenlanguages()) {
					insertSpokenLanguages.setInt(1, personid);
					insertSpokenLanguages.setString(2, truncateString(s, 64, "spokenlanguages.language"));
					insertSpokenLanguages.executeUpdate();
				}
			} finally {
				insertSpokenLanguages.close();
			}
		}

		// save family
		Set<FamilyMember> family;
		if((family = details.getFamily()) != null) {
			PreparedStatement insertFamilyMember = conn.prepareStatement("insert into familymember(personId, " +
					"familymemberId, role) values(?, ?, ?)");
			try {
				for(FamilyMember member : family) {
					insertFamilyMember.setInt(1, personid);
					int memberid = getPersonId(conn, member.getPerson().getId(), member.getPerson().getName());
					insertFamilyMember.setInt(2, memberid);
					insertFamilyMember.setString(3, truncateString(member.getRelation(), 45, "familymember.role"));
					insertFamilyMember.executeUpdate();
				}
			} finally {
				insertFamilyMember.close();
			}
		}

		// save education data
		List<EducationItem> education;
		if((education = details.getEducation()) != null) {
			PreparedStatement insertEducationItem = conn.prepareStatement("insert into educationitem(personId, " +
					"`index`, name, educationFbId, timeperiod, type, field) values(?, ?, ?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			try {
				int itemIdx = 0;
				for(EducationItem educ : education) {
					insertEducationItem.setInt(1, personid);
					insertEducationItem.setInt(2, itemIdx++);
					insertEducationItem.setString(3, truncateString(educ.getName(), 64, "educationitem.name"));
					insertEducationItem.setString(4, educ.getId());
					insertEducationItem.setString(5, truncateString(educ.getTimeperiod(), 45,
							"educationitem.timeperiod"));
					insertEducationItem.setString(6, truncateString(educ.getType(), 64, "educationitem.type"));
					insertEducationItem.setString(7, truncateString(educ.getField(), 256, "educationitem.field"));
					insertEducationItem.executeUpdate();
					int educationitemid;
					ResultSet res = insertEducationItem.getGeneratedKeys();
					try {
						res.next();
						educationitemid = res.getInt(1);
					} finally {
						res.close();
					}

					// If current education has courses, save them as well.
					if(educ.getClasses() == null)
						continue;
					PreparedStatement insertEducationClass = conn.prepareStatement(
							"insert into educationclass(parentId, `index`, name, description) values(?, ?, ?, ?)",
							Statement.RETURN_GENERATED_KEYS);
					try {
						int classIdx = 0;
						for(EducationClass course : educ.getClasses()) {
							insertEducationClass.setInt(1, educationitemid);
							insertEducationClass.setInt(2, classIdx++);
							insertEducationClass.setString(3, truncateString(course.getName(), 64,
									"educationclass.name"));
							insertEducationClass.setString(4, truncateString(course.getDescription(), 200,
									"educationclass.description"));
							insertEducationClass.executeUpdate();
							int educationclassid;
							ResultSet rset = insertEducationClass.getGeneratedKeys();
							try {
								rset.next();
								educationclassid = rset.getInt(1);
							} finally {
								rset.close();
							}

							// If course has associated people, save them as well.
							PreparedStatement insertEducationwithPerson = conn.prepareStatement(
									"insert into educationwithperson(educationclassId, personId) values(?, ?)");
							try {
								for(Person p : course.getPersons()) {
									int classpersonid = getPersonId(conn, p.getId(), p.getName());
									insertEducationwithPerson.setInt(1, educationclassid);
									insertEducationwithPerson.setInt(2, classpersonid);
									insertEducationwithPerson.executeUpdate();
								}
							} finally {
								insertEducationwithPerson.close();
							}
						}
					} finally {
						insertEducationClass.close();
					}
				}
			} finally {
				insertEducationItem.close();
			}
		}

		// Insert workplaces, if present.
		List<WorkItem> work;
		if((work = details.getWork()) != null) {
			PreparedStatement insertWorkItem = conn.prepareStatement("insert into workitem(personId, `index`, name, " +
					"workFbId, title, timeperiod, place, description) values(?, ?, ?, ?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			try {
				int itemIdx = 0;
				for(WorkItem w : work) {
					insertWorkItem.setInt(1, personid);
					insertWorkItem.setInt(2, itemIdx++);
					insertWorkItem.setString(3, truncateString(w.getName(), 256, "workitem.name"));
					insertWorkItem.setString(4, w.getId());
					insertWorkItem.setString(5, truncateString(w.getTitle(), 64, "workitem.title"));
					insertWorkItem.setString(6, truncateString(w.getTimeperiod(), 45, "workitem.timeperiod"));
					insertWorkItem.setString(7, truncateString(w.getPlace(), 45, "workitem.place"));
					insertWorkItem.setString(8, truncateString(w.getDescription(), 200, "workitem.description"));
					insertWorkItem.executeUpdate();
					int workitemid;
					ResultSet res = insertWorkItem.getGeneratedKeys();
					try {
						res.next();
						workitemid = res.getInt(1);
					} finally {
						res.close();
					}

					// If current workplace has associated projects, save them as well.
					if(w.getProjects() == null)
						continue;
					PreparedStatement insertWorkProject = conn.prepareStatement(
							"insert into workproject(parentId, `index`, name, timeperiod, description) " +
							"values(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
					try {
						int projectIdx = 0;
						for(WorkProject project : w.getProjects()) {
							insertWorkProject.setInt(1, workitemid);
							insertWorkProject.setInt(2, projectIdx++);
							insertWorkProject.setString(3, truncateString(project.getName(), 64, "workproject.name"));
							insertWorkProject.setString(4, truncateString(project.getTimeperiod(), 45,
									"workproject.timeperiod"));
							insertWorkProject.setString(5, truncateString(project.getDescription(), 200,
									"workproject.description"));
							insertWorkProject.executeUpdate();
							int workprojectid;
							ResultSet rset = insertWorkProject.getGeneratedKeys();
							try {
								rset.next();
								workprojectid = rset.getInt(1);
							} finally {
								rset.close();
							}

							// If the project has associated people, save them as well.
							PreparedStatement insertWorkwithPerson = conn.prepareStatement(
									"insert into workwithperson(workprojectId, personId) values(?, ?)");
							try {
								for(Person p : project.getPersons()) {
									int workprojectpersonid = getPersonId(conn, p.getId(), p.getName());
									insertWorkwithPerson.setInt(1, workprojectid);
									insertWorkwithPerson.setInt(2, workprojectpersonid);
									insertWorkwithPerson.executeUpdate();
								}
							} finally {
								insertWorkwithPerson.close();
							}
						}
					} finally {
						insertWorkProject.close();
					}
				}
			} finally {
				insertWorkItem.close();
			}
		}
	}

	/**
	 * Save Post in database and return PostId.
	 */
	private static int insertPost(Connection conn, Post element, int existingPostId, int index, String curUserId,
			String curUserName) throws SQLException {
		int type;	// post type; value 1 is reserved for Comment
		String userid;
		String name;
		Timestamp dateandtime;
		if(element instanceof NormalPost) {
			if (element instanceof EmbeddableNormalPost)
				type = 5;
			else
				type = 2;
			userid = ((NormalPost) element).getHeader().getSender().getId();
			name = ((NormalPost) element).getHeader().getSender().getName();
			dateandtime = convertDate(((NormalPost) element).getHeader().getTime());
		} else if(element instanceof SmallPost) {
			type = 3;
			userid = ((SmallPost) element).getHeader().getSender().getId();
			name = ((SmallPost) element).getHeader().getSender().getName();
			dateandtime = convertDate(((SmallPost) element).getHeader().getTime());
		} else if(element instanceof LifeEvent) {
			type = 4;
			userid = curUserId;
			name = curUserName;
			dateandtime = convertDate(((LifeEvent) element).getTime());
		} else
			throw new RuntimeException("unknown post type");

		String text = element.getText();

		// determine personid
		int personid = getPersonId(conn, userid, name);
		int wallid = getPersonId(conn, curUserId, curUserName);
		// save actual Post
		PreparedStatement st;
		if (existingPostId >= 0)
			st = conn.prepareStatement("update post set type=?, personId=?, wallId=?, `index`=?, date=?, text=? " +
					"where postId=?");
		else
			st = conn.prepareStatement("insert into post(type, personId, wallId, `index`, date, text) " +
					"values(?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		try {
			st.setInt(1, type);
			st.setInt(2, personid);
			st.setInt(3, wallid);
			st.setInt(4, index);
			st.setTimestamp(5, dateandtime);
			st.setString(6, text);
			if (existingPostId >= 0)
				st.setInt(7, existingPostId);
			st.executeUpdate();

			if (existingPostId < 0) {
				ResultSet res = st.getGeneratedKeys();
				try {
					res.next();
					return res.getInt(1);
				} finally {
					res.close();
				}
			}
		} finally {
			st.close();
		}
		return existingPostId;
	}

	/**
	 * Save comment in database and return postid.
	 */
	private static int insertPost(Connection conn, Comment element, int index, String curUserId, String curUserName)
			throws SQLException {
		String userid = element.getSender().getId();
		String name = element.getSender().getName();
		Timestamp dateandtime = convertDate(element.getTime());
		String text = element.getText();

		int personid = getPersonId(conn, userid, name);
		int wallid = getPersonId(conn, curUserId, curUserName);

		PreparedStatement insertComment = conn.prepareStatement("insert into post(type, personId, wallId, `index`, " +
				"date, text) values(1, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		try {
			insertComment.setInt(1, personid);
			insertComment.setInt(2, wallid);
			insertComment.setInt(3, index);
			insertComment.setTimestamp(4, dateandtime);
			insertComment.setString(5, text);
			insertComment.executeUpdate();
			ResultSet res = insertComment.getGeneratedKeys();
			try {
				res.next();
				return res.getInt(1);
			} finally {
				res.close();
			}
		} finally {
			insertComment.close();
		}
	}

}
