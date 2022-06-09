package edu.tum.cs.postprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.stream.Field;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.pff.PSTMessage;
import com.pff.PSTRecipient;

import edu.tum.cs.util.LanguageDetection;

public class HackingTeamExporter extends EmailProcessor {

	private static final Logger logger = Logger.getLogger(HackingTeamExporter.class.getName());

	// senders of automated mailings
	private static final String[] blacklistAddressesUnresolved = {
		"confluence@hackingteam",
		"support@hackingteam",
		"vmbackup@hackingteam",
		"avtest@hackingteam"
	};
	private static final Set<String> ignoredAddresses = new HashSet<String>();

	private static final String[] blacklistPst = {
		"support.pst"
	};
	private static final Set<String> ignoredPst = new HashSet<String>(Arrays.asList(blacklistPst));

	private final UserAliasList aliases;
	private final String targetLanguage;
	private final Map<String, SocialMediaUser> usersById = new HashMap<String, SocialMediaUser>();
	private final MessageAggregator aggregator = new MessageAggregator(true);
	private final EmailReplyChain replies = new EmailReplyChain();
	private int numProcessed = 0;

	public HackingTeamExporter(UserAliasList aliases, String targetLanguage) {
		this.aliases = aliases;
		this.targetLanguage = targetLanguage;
	}

	private static String detectLanguage(String text) {
		String lang = "unknown";
		if (!text.isEmpty()) {
			try {
				Detector languageDetector = DetectorFactory.create();
				languageDetector.append(text);
				lang = languageDetector.detect();
			} catch (LangDetectException ex) {
				// ignore
			}
		}
		return lang;
	}

	private SocialMediaUser resolveUser(String address, String name) {
		if (!((address.indexOf('@') > 0) || address.startsWith("/O="))) {
			if (!address.isEmpty())
				logger.warning("ignoring invalid address '" + address + "'");
			return null;
		}

		boolean isCore = false;
		String id = aliases.resolve(address);
		if (id == null)
			id = aliases.normalize(address);
		else if (id.startsWith("!"))
			id = id.substring(1);
		else
			isCore = true;
		if (ignoredAddresses.contains(id))
			return null;

		SocialMediaUser user = usersById.get(id);
		if (user == null) {
			user = new SocialMediaUser(id, (name != null) ? name : id, isCore, 0);
			usersById.put(id, user);
		}
		return user;
	}

	private static String extractParentId(PSTMessage msg, Header header) {
		// try "In-Reply-To" first
		String id = msg.getInReplyToId();
		if (!id.isEmpty())
			return id;

		// if not present, try "References"; last element is ID of parent
		Field field = header.getField("References");
		if (field != null) {
			id = field.getBody();
			int addrStartIdx = id.lastIndexOf('<');
			if (addrStartIdx >= 0) {
				int addrEndIdx = id.indexOf('>', addrStartIdx + 1);
				if (addrEndIdx >= 0)
					return id.substring(addrStartIdx, addrEndIdx + 1);
			}
		}

		return null;
	}

	private SocialMediaMessage importMessage(PSTMessage msg, Header header) throws Exception {
		// resolve sender
		SocialMediaUser sender = resolveUser(msg.getSenderEmailAddress(), msg.getSenderName());
		if (sender == null)
			return null;

		// resolve recipients
		int n = msg.getNumberOfRecipients();
		Set<SocialMediaUser> recipients = new HashSet<SocialMediaUser>(n);
		for (int i = 0; i < n; i++) {
			PSTRecipient msgRecip = msg.getRecipient(i);
			SocialMediaUser recipient = resolveUser(msgRecip.getEmailAddress(), msgRecip.getDisplayName());
			if (recipient != null)
				recipients.add(recipient);
		}
		if (recipients.isEmpty())
			return null;

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

		String body = msg.getBody();
		if (body.isEmpty()) {
			body = msg.getBodyHTML();
			if (body.isEmpty()) {
				ParsedRtf rtf = parseRtf(msg.getRTFBody());
				if (rtf.isEncapsulatedHtml)
					body = stripHtmlTags(rtf.getText());
				else
					body = rtf.getText();
			} else
				body = stripHtmlTags(body);
		}
		body = cleanMessageBody(body).trim();
		sb.append(body);
		String content = sb.toString();

		if ((targetLanguage != null) && !targetLanguage.equals(detectLanguage(content)))
			return null;

		// extract message ID (generate if missing) & reply chain (using the same heuristic for classifying messages as
		// replies / forwarding as in EnronExporter)
		String messageId = msg.getInternetMessageId();
		if (messageId.isEmpty())
			messageId = UUID.randomUUID().toString();
		messageId = messageId.toLowerCase();

		String parentId;
		Field field = header.getField("X-Forwarded-Message-Id");
		if (field != null) {
			parentId = field.getBody().trim();
			isReply = false;
			isShared = true;
		} else
			parentId = extractParentId(msg, header);
		if (parentId != null)
			replies.addMessageId(messageId, parentId.toLowerCase());

		// X-Thread-Info looks similar, but has a different internal format; header most likely generated by "Barracuda"
		field = header.getField("Thread-Index");
		if (field != null)
			replies.addThreadIndex(field.getBody().trim(), messageId);

		// fetch remaining metadata and create message object
		Date date = msg.getClientSubmitTime();	// value of "Date" header
		return new SocialMediaMessage(messageId, date, sender, recipients, isReply, null, isShared, null, content);
	}

	@Override
	protected void processEmlMessage(Message msg, String fileName) {
		logger.warning("ignoring '" + fileName + "'");
	}

	@Override
	protected void processPstMessage(PSTMessage msg, Header header, String parentFolderName, File f) throws Exception {
		SocialMediaMessage extractedMessage = importMessage(msg, header);
		if (extractedMessage != null)
			aggregator.addMessage(extractedMessage);
		numProcessed++;
	}

	@Override
	public void processFiles(File baseDir, Set<String> ignoredFiles) throws IOException {
		super.processFiles(baseDir, ignoredFiles);

		// resolve message ID reply chain
		for (Map.Entry<String, String> e : replies.getReplyChain().entrySet()) {
			SocialMediaMessage msg = aggregator.getMessageById(e.getKey());
			SocialMediaMessage msgTarget = aggregator.getMessageById(e.getValue());	// can be null
			if (msg.isShared())
				msg.updateOrigin(msgTarget);
			else
				msg.updateParent(msgTarget);
		}
	}

	@Override
	public String toString() {
		return "processed " + numProcessed + " messages\n" +
				"imported " + usersById.size() + " users and " + aggregator.getMessages().size() + " messages\n" +
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
			System.err.println("usage: " + HackingTeamExporter.class.getSimpleName() +
					" host database user password prefix aliases basedir [language]");
			return;
		}

		LanguageDetection.loadProfilesFromResources();
		String targetLanguage = null;
		if (args.length > 7)
			targetLanguage = args[7];

		UserAliasList aliases = new HackingTeamAliasList();
		aliases.readFromStream(new FileInputStream(args[5]));
		for (String address : blacklistAddressesUnresolved) {
			String canonicalName = aliases.resolve(address);
			ignoredAddresses.add((canonicalName != null) ? canonicalName : address);
		}

		HackingTeamExporter exp = new HackingTeamExporter(aliases, targetLanguage);
		exp.processFiles(new File(args[6]), ignoredPst);
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
