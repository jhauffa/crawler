package edu.tum.cs.postprocessing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.stream.Field;

import com.pff.PSTMessage;
import com.pff.PSTRecipient;

public class HackingTeamAliasExtractor extends EmailProcessor {

	// some "Sent" folders contains messages sent from the address of another user
	private static final String[] blacklistMessageIds = {
		"<53C680DD.1070303@hackingteam.com>", "<53C549AE.4000008@hackingteam.com>",
		"<53C548D9.8090006@hackingteam.com>", "<522D9BD6.2030505@bestlawyers.com>",
		"<522D9C3A.4000009@bestlawyers.com>", "<20080115165701.GA7550@rockrider.hackingteam.it>",
		"<54A2A66E.5080008@gmail.com>", "<4519394F.3070806@hackingteam.it>",
		"<004b01cec0dd$82b1d140$881573c0$@hackingteam.it>"
	};
	private static final Set<String> ignoredMessageIds = new HashSet<String>(Arrays.asList(blacklistMessageIds));

	// email addresses used as "From:" address, but not associated with a particular employee
	private static final String[] blacklistAddresses = {
		"foobar@hackingteam", "support@hackingteam", "rcs-support@hackingteam", "ad@hackingteam", "info@hackingteam"
	};
	private static final Set<String> ignoredAddresses = new HashSet<String>(Arrays.asList(blacklistAddresses));

	private static final String[] ignoredAddressParts = {
		"return", "news", "marketing", "campaign", "bounce", "bnc", "postmaster", "administrator", "mailer",
		"undelivered", "sema-cr", "reply", "replay", "localhost", "localdomain", "reverse", "smtp", "www-data", "root",
		"apache", "mailrelay", "error", "remove", "unsubscribe", "b.e.", "\"", "=", " ",
		"amazonses.com", "mktomail.com", "contactlab.it", "mdc.extra.it", "mandrillapp.com", "creatormailitalia.net",
		"criticalimpactinc.com", "constantcontact.com", "chtah.com", "dotmailer-email.com", "msgfocus.com",
		"bmsend.com", "cmail1", "cmail2", "cmail3", "emarsys.net", "createsend", "emailwarp.com", "mailing-lists.it",
		"bfi0.com", "retarus.com", "twobirds.com",
		"member@linkedin.com"
	};
	private static final String[] ignoredAddressRegexes = {
		".*(@|\\.)r[0-9][0-9]\\.it", ".*@sbr[0-9][0-9]\\.net", ".*@[a-z][a-z][0-9][0-9][0-9][a-z][a-z]\\.info"
	};
	private static final Pattern[] ignoredAddressPatterns;
	static {
		ignoredAddressPatterns = new Pattern[ignoredAddressRegexes.length];
		for (int i = 0; i < ignoredAddressRegexes.length; i++)
			ignoredAddressPatterns[i] = Pattern.compile(ignoredAddressRegexes[i]);
	}

	// manually identified mappings
	private static final String adPrefix =
			"/o=hackingteam/ou=exchange administrative group (fydibohf23spdlt)/cn=recipients/cn=";
	private static final String[][] manualMappings = {
		{ adPrefix + "emanuele placidiea3", "emanuele.placidi@gmail.com" },
		{ adPrefix + "fabrizio cornellib9d", "zeno@hackingteam", "fabrizio.cornelli@gmail.com" },
		{ adPrefix + "giovanni cino0e5", "giovanni.cino@gmail.com" },
		{ adPrefix + "massimo chiodiniddb", "kiodo@hackingteam", "max.chiodo@gmail.com" },
		{ adPrefix + "mauro romeof4d", "mauro.romeo@gmail.com" },
		{ adPrefix + "vtc55", "vt@seclab.it", "noreply@vt-community.com" },
		{ adPrefix + "fred d'alessioa1a", "fredd0104@aol.com" },
		{ adPrefix + "max hackingteam.it61d", "m.luppi@hackingteam" },
		{ adPrefix + "markoman hackingteam.it55b", "m.catino@hackingteam" },
		{ adPrefix + "emad hackingteam.itbe9", "emad@hackingteam" },
		{ adPrefix + "topac hackingteam.it2c1", "d.molteni@hackingteam" },
		{ adPrefix + "walter hackingteam.itd80", "walter@hackingteam", "w.furlan@hackingteam" },
	};

	private final UserAliasList aliases;
	private final UserAliasList secondaryAliases = new HackingTeamAliasList();

	public HackingTeamAliasExtractor(UserAliasList aliases) {
		this.aliases = aliases;
	}

	private static String extractAddressFromHeader(Header header, String key) {
		Field field = header.getField(key);
		if (field != null) {
			String value = field.getBody();
			int startPos = value.indexOf('<');
			if (startPos >= 0) {
				startPos += 1;
				int endPos = value.indexOf('>');
				if (endPos > startPos)
					return value.substring(startPos, endPos);
			} else if (value.contains("@"))
				return value;
		}
		return "";
	}

	private static String filterEmailAddress(String address) {
		if (address.startsWith("/O="))
			return address;
		String addressLower = address.toLowerCase();
		for (String ignoredPart : ignoredAddressParts)
			if (addressLower.contains(ignoredPart))
				return "";
		for (Pattern ignoredPattern : ignoredAddressPatterns)
			if (ignoredPattern.matcher(addressLower).matches())
				return "";
		return address;
	}

	@Override
	protected void processPstMessage(PSTMessage msg, Header header, String parentFolderName, File f) throws Exception {
		// check if sender is also recipient
		String senderAddress = msg.getSenderEmailAddress();
		String headerFromAddress = extractAddressFromHeader(header, "From");
		String headerToAddress = extractAddressFromHeader(header, "To");
		String headerReplyToAddress = extractAddressFromHeader(header, "Reply-To");
		boolean toIsReplyTo = headerToAddress.equals(headerReplyToAddress);
		boolean senderIsRecipient = false;
		for (int i = 0; i < msg.getNumberOfRecipients(); i++)
			if (msg.getRecipient(i).getEmailAddress().equals(senderAddress))
				senderIsRecipient = true;
		if (!senderIsRecipient)
			senderIsRecipient = headerFromAddress.equals(headerToAddress);

		// no alias detection in blacklisted and resent messages
		if (ignoredMessageIds.contains(msg.getInternetMessageId()) ||
			msg.getTransportMessageHeaders().contains("Resent-From:")) {
			return;
		}

		// find aliases
		String fileName = f.getName();
		String canonicalName = fileName.substring(0, fileName.lastIndexOf('.'));
		if (parentFolderName.startsWith("Sent") || parentFolderName.startsWith("Posta inviata") ||
				parentFolderName.startsWith("Outgoing") || parentFolderName.startsWith("Posta in uscita") ||
				parentFolderName.startsWith("Drafts") || parentFolderName.startsWith("Bozze")) {
			if (!senderAddress.isEmpty() &&
				!ignoredAddresses.contains(aliases.normalize(senderAddress)) &&
				!msg.getTransportMessageHeaders().contains("Received:")) {
				aliases.add(canonicalName, Arrays.asList(senderAddress));
			}
		}

		// exploit redundancy in PST meta-data and transport headers to find some more aliases
		if (!senderIsRecipient && !toIsReplyTo &&
			!senderAddress.endsWith("hackingteam.com") && !senderAddress.endsWith("hackingteam.it")) {
			Set<String> senderAliases = new HashSet<String>();
			senderAliases.add(filterEmailAddress(senderAddress));
			senderAliases.add(filterEmailAddress(msg.getSentRepresentingEmailAddress()));
			senderAliases.add(filterEmailAddress(headerFromAddress));
			senderAliases.add(filterEmailAddress(extractAddressFromHeader(header, "Sender")));
			senderAliases.add(filterEmailAddress(extractAddressFromHeader(header, "Return-Path")));
			// not using Reply-To, causes too many weird false positives

			senderAliases.remove("");
			if (senderAliases.size() > 1)
				secondaryAliases.merge(senderAliases);
		}

		for (int i = 0; i < msg.getNumberOfRecipients(); i++) {
			PSTRecipient recip = msg.getRecipient(i);
			String[] addr = new String[2];
			addr[0] = recip.getEmailAddress();
			addr[1] = recip.getSmtpAddress();
			if (!addr[0].isEmpty() && !addr[1].isEmpty() && !addr[0].equalsIgnoreCase(addr[1]))
				secondaryAliases.merge(Arrays.asList(addr));
		}
	}

	@Override
	protected void processEmlMessage(Message msg, String fileName) {
		System.err.println("ignoring '" + fileName + "'");
	}

	@Override
	protected void processPstFile(File f) throws IOException {
		super.processPstFile(f);

		String fileName = f.getName();
		String canonicalName = fileName.substring(0, fileName.lastIndexOf('.'));
		if (aliases.hasAliases(canonicalName)) {
			String aliasName = canonicalName + "@hackingteam";
			aliases.add(canonicalName, Arrays.asList(aliasName));
		}
	}

	@Override
	public void processFiles(File baseDir, Set<String> ignoredFiles) throws IOException {
		super.processFiles(baseDir, ignoredFiles);
		for (Set<String> aliasNames : secondaryAliases.getAliases().values())
			aliases.merge(aliasNames);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("usage: " + HackingTeamAliasExtractor.class.getSimpleName() + " basedir");
			return;
		}

		UserAliasList aliases = new HackingTeamAliasList();
		HackingTeamAliasExtractor proc = new HackingTeamAliasExtractor(aliases);
		proc.processFiles(new File(args[0]));
		for (String[] aliasNames : manualMappings)
			aliases.merge(Arrays.asList(aliasNames));
		aliases.clean();
		System.out.print(aliases);
	}

}
