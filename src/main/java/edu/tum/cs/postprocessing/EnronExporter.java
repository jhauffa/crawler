package edu.tum.cs.postprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.james.mime4j.dom.Body;
import org.apache.james.mime4j.dom.Entity;
import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.Multipart;
import org.apache.james.mime4j.dom.TextBody;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.stream.Field;
import org.apache.james.mime4j.util.MimeUtil;

import com.pff.PSTMessage;

public class EnronExporter extends EmailProcessor {

	private static final Logger logger = Logger.getLogger(EnronExporter.class.getName());

	private final UserAliasList aliases;
	private final Map<String, SocialMediaUser> usersById = new HashMap<String, SocialMediaUser>();
	private final MessageAggregator aggregator = new MessageAggregator(true);
	private final EmailReplyChain replies = new EmailReplyChain();
	private int numProcessed = 0;
	private int numMissingUniqueId = 0;

	public EnronExporter(UserAliasList aliases) {
		this.aliases = aliases;
	}

	private SocialMediaUser mailboxToUser(Mailbox mailbox) {
		// Ensure that there is some kind of unique identifier, either an email address, or an LDAP path; some messages
		// have sender or recipient lists that only contain person names, which cannot be properly disambiguated.
		String address = EnronAliasList.untransformAddress(mailbox.getAddress());
		if (!((address.indexOf('@') > 0) || address.startsWith("/O=")))
			return null;

		boolean isCore = false;
		String id = aliases.resolve(address);
		if (id == null)
			id = aliases.normalize(address);
		else if (id.startsWith("!"))
			id = id.substring(1);
		else
			isCore = true;

		SocialMediaUser user = usersById.get(id);
		if (user == null) {
			String name = mailbox.getName();
			user = new SocialMediaUser(id, (name != null) ? name : id, isCore, 0);
			usersById.put(id, user);
		}
		return user;
	}

	private static void handleTextBody(TextBody body, String mimeType, StringBuilder sb) throws IOException {
		String text;
		Reader r = body.getReader();
		try {
			text = IOUtils.toString(r);
		} finally {
			r.close();
		}
		if (mimeType.startsWith("text/html") ||
			text.startsWith("<html") || text.startsWith("<HTML") || text.startsWith("<!"))
			text = stripHtmlTags(text);
		sb.append(text);
	}

	private static void handleMultipart(Multipart body, StringBuilder sb) throws IOException {
		Entity bestAlternative = null;
		int bestScore = 0;
		for (Entity alternative : body.getBodyParts()) {
			String mimeType = alternative.getMimeType();
			int score = 0;
			if (mimeType.startsWith("text/")) {
				if (mimeType.startsWith("text/plain"))
					score = 4;
				else if (mimeType.startsWith("text/html"))
					score = 3;
				else
					score = 2;
			} else if (mimeType.startsWith("multipart"))
				score = 1;

			if (score > bestScore) {
				bestAlternative = alternative;
				bestScore = score;
			}
		}
		if (bestAlternative != null)
			extractText(bestAlternative, sb);
	}

	private static void extractText(Entity entity, StringBuilder sb) throws IOException {
		String mimeType = entity.getMimeType();
		Body body = entity.getBody();
		if (mimeType.startsWith("text/"))
			handleTextBody((TextBody) body, mimeType, sb);
		else if (mimeType.startsWith("multipart"))
			handleMultipart((Multipart) body, sb);
	}

	private static String extractParentId(Message msg) {
		// try "In-Reply-To"
		Field field = msg.getHeader().getField("In-Reply-To");
		if (field != null)
			return field.getBody().trim();

		// try "References"; last element is ID of parent
		field = msg.getHeader().getField("References");
		if (field != null) {
			String value = field.getBody();
			int addrStartIdx = value.lastIndexOf('<');
			if (addrStartIdx >= 0) {
				int addrEndIdx = value.indexOf('>', addrStartIdx + 1);
				if (addrEndIdx >= 0)
					return value.substring(addrStartIdx, addrEndIdx + 1);
			}
		}

		return null;
	}

	private SocialMediaMessage importMessage(Message msg) throws IOException {
		// resolve sender
		List<Mailbox> senderMailboxes = msg.getFrom();
		if (senderMailboxes == null)
			return null;
		// There should only be one sender, but sometimes the header is messed up, and mime4j will tokenize it as
		// multiple mailbox specifiers; take the first one that looks like a valid address.
		SocialMediaUser sender = null;
		for (Mailbox senderMailbox : senderMailboxes) {
			sender = mailboxToUser(senderMailbox);
			if (sender != null)
				break;
		}
		if (sender == null) {
			numMissingUniqueId++;
			return null;
		}

		// resolve recipients
		List<Mailbox> recipientMailboxes = new ArrayList<Mailbox>();
		if (msg.getTo() != null)
			recipientMailboxes.addAll(msg.getTo().flatten());
		if (msg.getCc() != null)
			recipientMailboxes.addAll(msg.getCc().flatten());
		if (msg.getBcc() != null)
			recipientMailboxes.addAll(msg.getBcc().flatten());
		Set<SocialMediaUser> recipients = new HashSet<SocialMediaUser>();
		for (Mailbox recipientMailbox : recipientMailboxes) {
			SocialMediaUser recipient = mailboxToUser(recipientMailbox);
			if (recipient != null)
				recipients.add(recipient);
		}
		if (recipients.isEmpty()) {
			if (!recipientMailboxes.isEmpty())
				numMissingUniqueId++;
			return null;
		}

		// concatenate and clean all text parts of the message body
		boolean isReply = false;
		boolean isShared = false;
		StringBuilder sb = new StringBuilder();
		String subject = msg.getSubject();
		if (subject != null) {
			subject = subject.trim();
			isReply = hasReplyPrefix(subject);
			isShared = hasForwardPrefix(subject);
			sb.append(subject).append('\n');
		}
		extractText(msg, sb);
		String content = cleanMessageBody(sb.toString()).trim();

		// extract message ID (generate if missing) & reply chain
		String messageId = msg.getMessageId();
		if (messageId == null)
			messageId = MimeUtil.createUniqueMessageId("enron");
		messageId = messageId.toLowerCase();

		// Retrieve the ID of the parent message in the reply/forwarding chain, if present. The headers "References" and
		// "In-Reply-To" are equally used for replying and forwarding. Only if "X-Forwarded-Message-Id" present, we can
		// be sure it is forwarding, otherwise we have to guess using the subject prefix
		// (c.f. https://bugzilla.mozilla.org/show_bug.cgi?id=583587).
		String parentId;
		Field field = msg.getHeader().getField("X-Forwarded-Message-Id");
		if (field != null) {
			parentId = field.getBody().trim();
			isReply = false;
			isShared = true;
		} else
			parentId = extractParentId(msg);
		if (parentId != null)
			replies.addMessageId(messageId, parentId.toLowerCase());

		// Microsoft has its own threading system, which does not use the message ID...
		field = msg.getHeader().getField("Thread-Index");
		if (field != null)
			replies.addThreadIndex(field.getBody().trim(), messageId);

		// fetch remaining metadata and create message object
		Date date = msg.getDate();
		if (date == null)
			return null;
		return new SocialMediaMessage(messageId, date, sender, recipients, isReply, null, isShared, null, content);
	}

	@Override
	protected void processEmlMessage(Message msg, String fileName) throws IOException {
		SocialMediaMessage extractedMessage = importMessage(msg);
		if (extractedMessage != null)
			aggregator.addMessage(extractedMessage);
		numProcessed++;
	}

	@Override
	protected void processPstMessage(PSTMessage msg, Header header, String parentFolderName, File f) {
		logger.warning("ignoring '" + f.getPath() + "'");
	}

	@Override
	public void processFiles(File baseDir, Set<String> ignoredFiles) throws IOException {
		super.processFiles(baseDir, ignoredFiles);

		// resolve message ID reply chain
		for (Map.Entry<String, String> e : replies.getReplyChain().entrySet()) {
			SocialMediaMessage msg = aggregator.getMessageById(e.getKey());
			if (msg.isShared())
				msg.updateOrigin(aggregator.getMessageById(e.getValue()));
			else
				msg.updateParent(aggregator.getMessageById(e.getValue()));
		}
	}

	@Override
	public String toString() {
		return "processed " + numProcessed + " EML files, " + numMissingUniqueId +
				" missing unique identifiers for sender/recipients\n" +
				"read " + usersById.size() + " users and " + aggregator.getMessages().size() + " messages\n" +
				aggregator;
	}

	public Collection<SocialMediaUser> getUsers() {
		return usersById.values();
	}

	public Collection<SocialMediaMessage> getMessages() {
		return aggregator.getMessages();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			System.err.println("usage: " + EnronExporter.class.getSimpleName() +
					" host database user password prefix aliases basedir");
			return;
		}

		UserAliasList aliases = new EnronAliasList();
		aliases.readFromStream(new FileInputStream(args[5]));

		EnronExporter exp = new EnronExporter(aliases);
		exp.processFiles(new File(args[6]));
		System.out.println(exp);

		SocialMediaDao out = new SocialMediaDao(args[0], args[1], args[2], args[3], args[4]);
		try {
			out.save(exp.getUsers(), exp.getMessages());
		} finally {
			out.shutdown();
		}
		System.out.println("done");
	}

}
