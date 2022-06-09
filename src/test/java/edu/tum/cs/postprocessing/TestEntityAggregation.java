package edu.tum.cs.postprocessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

import org.junit.Test;

public class TestEntityAggregation {

	private static class DummyUserAliasList extends UserAliasList {
		@Override
		public String normalize(String name) {
			return name.toLowerCase();
		}
	}

	private static final String canonicalName = "foo";
	private static final String[] aliasNames = {
		"John Smith", "Johann von Smithenstein", "jsmith", "Johann S.", "j-bro"
	};

	@Test
	public void testUserAliasList() throws IOException {
		UserAliasList aliases = new DummyUserAliasList();
		aliases.add(canonicalName, Arrays.asList(new String[] { aliasNames[0], aliasNames[1] }));
		aliases.add(canonicalName, Arrays.asList(new String[] { aliasNames[2] }));
		aliases.merge(Arrays.asList(new String[] { aliasNames[2], aliasNames[3] }));
		aliases.merge(Arrays.asList(new String[] { aliasNames[4] } ));	// creates new temporary canonical name
		aliases.merge(Arrays.asList(new String[] { aliasNames[2], aliasNames[4] } ));	// merges temporary canon. name

		boolean gotException = false;
		try {
			aliases.add(canonicalName + "bar", Arrays.asList(new String[] { aliasNames[0] }));
		} catch (IllegalStateException ex) {
			gotException = true;
		}
		assertTrue(gotException);

		for (String aliasName : aliasNames) {
			assertEquals(aliasName, canonicalName, aliases.resolve(aliasName));
			assertEquals(aliasName + " (uc)", canonicalName, aliases.resolve(aliasName.toUpperCase()));
		}
		assertNull(aliases.resolve(canonicalName));

		String aliasStr = aliases.toString();
		ByteArrayInputStream is = new ByteArrayInputStream(aliasStr.getBytes());
		UserAliasList aliases2 = new DummyUserAliasList();
		aliases2.readFromStream(is);
		for (String aliasName : aliasNames)
			assertEquals(aliasName, canonicalName, aliases2.resolve(aliasName));
	}

	@Test
	public void testMessageAggregator() {
		Date refDate = new Date();
		String refText = "some text";
		SocialMediaUser[] users = new SocialMediaUser[4];
		for (int i = 0; i < users.length; i++)
			users[i] = new SocialMediaUser("user" + (i + 1), "User " + (i + 1), true, 0);

		// test whitespace-sensitive aggregation
		MessageAggregator aggregator = new MessageAggregator(false);
		int msgId = 0;
		SocialMediaMessage origMsg = new SocialMediaMessage("msg" + msgId, refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })), refText);
		SocialMediaMessage resMsg = aggregator.addMessage(origMsg);
		assertTrue(resMsg == origMsg);

		// exact duplicate, same message ID
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })), refText));
		assertTrue(resMsg == origMsg);

		// exact duplicate, different message ID
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })), refText));
		assertTrue(resMsg == origMsg);

		// duplicate, merge recipient lists
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[2] })), refText));
		assertTrue(resMsg == origMsg);

		// non-duplicates (different date, different sender, different content)
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), new Date(1337), users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[3] })), refText));
		assertTrue(resMsg != origMsg);
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[1],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[3] })), refText));
		assertTrue(resMsg != origMsg);
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[3] })), refText + "!"));
		assertTrue(resMsg != origMsg);
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[3] })),
				"\n \n   " + refText + "\r\n"));
		assertTrue(resMsg != origMsg);

		// empty message body
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (msgId++), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })), ""));
		assertTrue(resMsg == null);

		assertEquals(5, aggregator.getMessages().size());
		assertEquals(1, aggregator.getNumEmpty());
		assertEquals(1, aggregator.getNumMerged());
		assertEquals(2, aggregator.getNumDuplicate());

		assertEquals("msg0", aggregator.getMessageById("msg0").getId());
		assertEquals("msg0", aggregator.getMessageById("msg1").getId());
		assertEquals("msg0", aggregator.getMessageById("msg2").getId());
		assertEquals("msg3", aggregator.getMessageById("msg3").getId());
		assertEquals("msg4", aggregator.getMessageById("msg4").getId());
		assertEquals("msg5", aggregator.getMessageById("msg5").getId());
		assertEquals("msg6", aggregator.getMessageById("msg6").getId());
		assertNull(aggregator.getMessageById("msg7"));

		boolean containsMergedMessage = false;
		for (SocialMediaMessage msg : aggregator.getMessages()) {
			if (msg.getDate().equals(refDate) && msg.getContent().equals(refText) && msg.getSender().equals(users[0]) &&
				(msg.getRecipients().size() == 2) &&
				msg.getRecipients().contains(users[1]) && msg.getRecipients().contains(users[2])) {
				containsMergedMessage = true;
				break;
			}
		}
		assertTrue(containsMergedMessage);

		// test whitespace-insensitive aggregation
		aggregator = new MessageAggregator(true);
		origMsg = new SocialMediaMessage("msg" + msgId, refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })),
				"line 1\nline 2\nline 3");
		resMsg = aggregator.addMessage(origMsg);
		assertTrue(resMsg == origMsg);
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (++msgId), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })),
				"line 1     \nline2\r\nline 3\n\n    \n"));
		assertTrue(resMsg == origMsg);
		resMsg = aggregator.addMessage(new SocialMediaMessage("msg" + (++msgId), refDate, users[0],
				new HashSet<SocialMediaUser>(Arrays.asList(new SocialMediaUser[] { users[1] })),
				"something entirely different"));
		assertTrue(resMsg != origMsg);

		assertEquals(2, aggregator.getMessages().size());
		assertEquals(0, aggregator.getNumEmpty());
		assertEquals(0, aggregator.getNumMerged());
		assertEquals(1, aggregator.getNumDuplicate());
	}

}
