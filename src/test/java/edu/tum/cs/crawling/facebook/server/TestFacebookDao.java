package edu.tum.cs.crawling.facebook.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.tum.cs.crawling.facebook.entities.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFacebookDao {

	/*
	 * create database crawler_unit_test;
	 * grant all on crawler_unit_test.* to 'unittest'@'localhost' identified by 'test';
	 * MySQL has to be configured for full UTF-8 support: character-set-server = utf8mb4
	 */
	private static final String dbName = "crawler_unit_test";
	private static final String dbUserName = "unittest";
	private static final String dbPassword = "test";

	private static final int numFakeProfiles = 2;

	private static final String[] tables = {
		"sitelikes", "workwithperson", "workproject", "workitem", "educationwithperson", "educationclass",
		"educationitem", "familymember", "spokenlanguages", "persondetails", "friendswith", "withperson",
		"mentionedperson", "mentionedsite", "lifeevent", "comment", "likes", "normalpost", "embeddablepost", "photo",
		"photoalbum", "link", "video", "embeddableobject", "post", "site", "place", "person"
	};

	private static Connection conn;

	@BeforeClass
	public static void prepareDatabase() {
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost/" + dbName, dbUserName, dbPassword);
		} catch (SQLException ex) {
			System.err.println("error connecting to test database");
			ex.printStackTrace();
		}
	}

	@Before
	public void initializeDao() throws Exception {
		if (conn != null)
			FacebookDao.initialize("localhost", dbName, dbUserName, dbPassword);
	}

	@After
	public void shutdownDao() throws SQLException {
		if (conn != null)
			FacebookDao.shutdown();
	}

	@AfterClass
	public static void cleanUpDatabase() throws SQLException {
		if (conn == null)
			return;

		try {
			// drop all generated tables
			Statement st = conn.createStatement();
			for (String table : tables) {
				try {
					st.executeUpdate("drop table " + table);
				} catch (SQLException ex) {
					System.err.println("error deleting table '" + table + "' from test database: " + ex.getMessage());
				}
			}
		} finally {
			conn.close();
		}
	}

	private static UserProfile buildFakeProfile(String id) {
		Set<Person> friends = new HashSet<Person>();
		friends.add(new Person("20", "Ina Müller"));
		friends.add(new Person("21", "Helene Fischer"));
		Set<Site> sites = new HashSet<Site>();
		sites.add(new Site("111", "Kenny G Fanclub"));
		sites.add(new Site("122", "Bobby D Fanclub"));
		Person user = new Person(id, "Hans " + id);
		Set<FamilyMember> family = new HashSet<FamilyMember>();
		family.add(new FamilyMember(new Person("12", "Petra Fakenhausen"), "Tante"));
		family.add(new FamilyMember(new Person("13", "Heinz Fakenhausen"), "Onkel"));
		List<EducationItem> education = new ArrayList<EducationItem>();
		List<EducationClass> classes = new ArrayList<EducationClass>();
		classes.add(new EducationClass("Einführung in die Horologie", "wirklich anspruchsvolles Zeug", friends));
		education.add(new EducationItem("Ingenieur", "17", "AK 2003", "Diplom", "Angewandte Horologie", classes));
		Set<String> languages = new HashSet<String>();
		languages.add("Deutsch");
		languages.add("Gallizisch");
		List<WorkItem> work = new ArrayList<WorkItem>();
		List<WorkProject> projects = new ArrayList<WorkProject>();
		projects.add(new WorkProject("Einführung einer computergestützten Krücke", "viel zu lange",
				"was für ein Blödsinn", friends));
		work.add(new WorkItem("Softweich", "666", "Sachbearbeiter für schwierige Fälle", "seit gestern",
				"Unterschleißheim", "nicht besonders anspruchsvolles Zeug", projects));
		PersonDetails details = new PersonDetails("toller Typ", "Semper fidelis.", 'm', 'f',
				new Date(System.currentTimeMillis() - (1000L * 60 * 100)), languages, "Pastafarier", "1338",
				"ultralinks \uD83D\uDE01" /* test handling of 4-byte UTF-8 sequences */, "-1", "0151-666667",
				"Tolle Straße 10, 49999 Tolle Stadt", "http://www.toll.de", "bla@bla.de", "Verheiratet",
				"bei meiner Mudda", "1444", "Hinzenhausen", "26666", family, education, work);
		EmbeddableNormalPost post = new EmbeddableNormalPost(new NormalPostHeader("post" + id,
				NormalPostHeader.Type.UPLOADEDVIDEO, user, new Date(), new Place("560", "im Garten"), null, friends),
				"mein tolles Video, schaut mal", new Video("1234", "mein Video, ey", "schau es dir an"), friends,
				sites);
		post.getComments().add(new Comment(friends.iterator().next(), new Date(), "voll uncool"));
		List<Post> posts = new ArrayList<Post>();
		posts.add(post);
		posts.add(new LifeEvent("Hochzeit", new Date(), "hab meine Alte geheiratet", "ehrlich getz"));
		posts.add(new SmallPost(new SmallPostHeader(friends.iterator().next(), new Date()), "wat soll dat?"));
		return new UserProfile(user, details, posts, friends.size(), friends, sites, "de");
	}

	@Test
	public void testSaveProfile() throws Exception {
		if (conn == null) {
			System.err.println("database not available, skipping test");
			return;
		}

		// save multiple profiles
		for (int i = 0; i < numFakeProfiles; i++)
			FacebookDao.saveUserProfile(buildFakeProfile("Fake" + i));

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("select count(*) from person");
		rs.next();
		assertEquals(4 + numFakeProfiles, rs.getInt(1));
		rs.close();
		rs = st.executeQuery("select count(*) from video");
		rs.next();
		assertEquals(1, rs.getInt(1));
		rs.close();
		rs = st.executeQuery("select count(*) from withperson");
		rs.next();
		assertEquals(2 * numFakeProfiles, rs.getInt(1));
		rs.close();
		rs = st.executeQuery("select politics from persondetails limit 1");
		rs.next();
		assertTrue(rs.getString(1).endsWith("\uD83D\uDE01"));
		rs.close();
	}

	@Test
	public void testStubReplacement() throws Exception {
		if (conn == null) {
			System.err.println("database not available, skipping test");
			return;
		}

		Person sharingUser = new Person("100", "sharing user");
		Person originalPoster = new Person("101", "original poster");
		PersonDetails emptyPersonDetails = new PersonDetails("", "", 'u', 'u', (Date) null,
				Collections.<String>emptySet(), "", "", "", "", "", "", "", "", "", "", "", "", "",
				Collections.<FamilyMember>emptySet(), Collections.<EducationItem>emptyList(),
				Collections.<WorkItem>emptyList());
		List<Post> sharingUsersPosts = new ArrayList<Post>();
		sharingUsersPosts.add(new EmbeddableNormalPost(new NormalPostHeader("sr100",
				NormalPostHeader.Type.NONE, sharingUser, new Date(), null, originalPoster, null),
				"finde ich voll gut", new EmbeddableNormalPost(new NormalPostHeader("sr101", originalPoster),
						"BANANEN!!!", null, null, null), null, null));
		UserProfile sharingUserProfile = new UserProfile(sharingUser, emptyPersonDetails,
				sharingUsersPosts, 0, Collections.<Person>emptySet(), Collections.<Site>emptySet(), "de");
		FacebookDao.saveUserProfile(sharingUserProfile);

		List<Post> originalUsersPosts = new ArrayList<Post>();
		originalUsersPosts.add(new EmbeddableNormalPost(new NormalPostHeader("sr101", NormalPostHeader.Type.NONE,
				originalPoster, new Date(), new Place("561", "Dortmund"), null, null), "BANANEN!!!", null, null,
				null));
		UserProfile originalPosterProfile = new UserProfile(originalPoster, emptyPersonDetails, originalUsersPosts,
				0, Collections.<Person>emptySet(), Collections.<Site>emptySet(), "de");
		FacebookDao.saveUserProfile(originalPosterProfile);

		Person otherSharingUser = new Person("102", "other sharing user");
		List<Post> otherSharingUsersPosts = new ArrayList<Post>();
		otherSharingUsersPosts.add(new EmbeddableNormalPost(new NormalPostHeader("sr102",
				NormalPostHeader.Type.NONE, sharingUser, new Date(), null, originalPoster, null),
				"finde ich auch voll gut", new EmbeddableNormalPost(new NormalPostHeader("sr101", originalPoster),
						"BANANEN!!!", null, null, null), null, null));
		UserProfile otherSharingUserProfile = new UserProfile(otherSharingUser, emptyPersonDetails,
				otherSharingUsersPosts, 0, Collections.<Person>emptySet(), Collections.<Site>emptySet(), "de");
		FacebookDao.saveUserProfile(otherSharingUserProfile);

		Statement st = conn.createStatement();
		// only count the posts created by the current test case
		ResultSet rs = st.executeQuery("select count(*) from embeddablepost, embeddableobject " +
				"where embeddableobject.objectFbID like \"sr%\" and embeddablepost.objectId=embeddableobject.objectId");
		rs.next();
		assertEquals(3, rs.getInt(1));
		rs.close();
		rs = st.executeQuery("select embeddablepost.placeId from embeddablepost, embeddableobject " +
				"where embeddableobject.objectFbId=\"sr101\" and embeddablepost.objectId=embeddableobject.objectId");
		rs.next();
		assertNotEquals(0, rs.getInt(1));
		assertFalse(rs.wasNull());
		rs.close();
		rs = st.executeQuery("select count(*) from post,person where post.personId=person.personId and " +
				"person.personFbId=\"101\"");
		rs.next();
		assertEquals(1, rs.getInt(1));	// there should be only one post by the original poster
		rs.close();
	}

	@Test
	public void testPostsToOtherTimeline() throws Exception {
		if (conn == null) {
			System.err.println("database not available, skipping test");
			return;
		}

		Person postingUser = new Person("110", "poster");
		Person timelineOwner = new Person("111", "target timeline owner");
		PersonDetails emptyPersonDetails = new PersonDetails("", "", 'u', 'u', (Date) null,
				Collections.<String>emptySet(), "", "", "", "", "", "", "", "", "", "", "", "", "",
				Collections.<FamilyMember>emptySet(), Collections.<EducationItem>emptyList(),
				Collections.<WorkItem>emptyList());

		// post appears on both timelines
		EmbeddableNormalPost post = new EmbeddableNormalPost(new NormalPostHeader("tl100",
				NormalPostHeader.Type.POSTONTO, postingUser, new Date(), null, null, null), "ey hömma",
				null, null, null);
		List<Post> posts = new ArrayList<Post>();
		posts.add(post);

		UserProfile profile = new UserProfile(postingUser, emptyPersonDetails, posts, 0,
				Collections.<Person>emptySet(), Collections.<Site>emptySet(), "de");
		FacebookDao.saveUserProfile(profile);
		profile = new UserProfile(timelineOwner, emptyPersonDetails, posts, 0,
				Collections.<Person>emptySet(), Collections.<Site>emptySet(), "de");
		FacebookDao.saveUserProfile(profile);

		Statement st = conn.createStatement();
		// only count the posts created by the current test case
		ResultSet rs = st.executeQuery("select count(*) from embeddablepost, embeddableobject " +
				"where embeddableobject.objectFbID like \"tl%\" and embeddablepost.objectId=embeddableobject.objectId");
		rs.next();
		assertEquals(2, rs.getInt(1));
		rs.close();
		// there should only be one objectId for both posts
		rs = st.executeQuery("select count(*) from embeddableobject where embeddableobject.objectFbID like \"tl%\"");
		rs.next();
		assertEquals(1, rs.getInt(1));
		rs.close();
	}

}
