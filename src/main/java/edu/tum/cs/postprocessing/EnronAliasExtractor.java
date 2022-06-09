package edu.tum.cs.postprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.stream.Field;

import com.pff.PSTMessage;

public class EnronAliasExtractor extends EmailProcessor {

	private static final String[] blacklistMessageIds = {
		"<4ED2FDEA6B4AF844878144F5C1B4E1914391C4@NAHOU-MSMBX07V.corp.enron.com>"
	};
	private static final Set<String> ignoredMessageIds = new HashSet<String>(Arrays.asList(blacklistMessageIds));

	private final UserAliasList aliases;

	public EnronAliasExtractor(UserAliasList aliases) {
		this.aliases = aliases;
	}

	private static String extractAddress(Message msg, String fieldName, boolean fallback) {
		String addr = null;
		Field field = msg.getHeader().getField(fieldName);
		if (field != null) {
			String value = field.getBody();
			int addrStartIdx = value.indexOf('<');
			int addrEndIdx = value.indexOf('>', addrStartIdx + 1);
			if ((addrStartIdx >= 0) && (addrEndIdx >= 0))
				addr = value.substring(addrStartIdx + 1, addrEndIdx);
			else if (fallback)
				addr = value.trim();
		}
		return addr;
	}

	private static void registerEmailAddress(Set<String> c, String address) {
		if (address != null) {
			address = EnronAliasList.untransformAddress(address);
			if ((address.contains("@") || address.startsWith("/O=")) && !address.contains("bounce"))
				c.add(address);
		}
	}

	private static Collection<String> getFromAddresses(Message msg) {
		Set<String> fromAddresses = new HashSet<String>();

		registerEmailAddress(fromAddresses, extractAddress(msg, "X-ZL-From", true));

		MailboxList senderList = msg.getFrom();
		if ((senderList == null) || (senderList.size() > 1))
			registerEmailAddress(fromAddresses, extractAddress(msg, "From", false));
		else
			registerEmailAddress(fromAddresses, senderList.get(0).getAddress());

		return fromAddresses;
	}

	@Override
	protected void processEmlMessage(Message msg, String fileName) {
		if (!ignoredMessageIds.contains(msg.getMessageId())) {	// no alias detection in blacklisted messages
			Collection<String> fromAddresses = getFromAddresses(msg);
			if (fromAddresses.size() > 1)
				aliases.merge(fromAddresses);
		}
	}

	@Override
	protected void processPstMessage(PSTMessage msg, Header header, String parentFolderName, File f) {
		System.err.println("ignoring '" + f.getPath() + "'");
	}

	// extract aliases from EML files and merge with existing list of "custodians", i.e. mailboxes
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("usage: " + EnronAliasExtractor.class.getSimpleName() + " aliases basedir");
			return;
		}

		UserAliasList aliases = new EnronAliasList();
		aliases.readFromStream(new FileInputStream(args[0]));
		EnronAliasExtractor proc = new EnronAliasExtractor(aliases);
		proc.processFiles(new File(args[1]));
		aliases.clean();
		System.out.print(aliases);
	}

}
