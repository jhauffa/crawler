package edu.tum.cs.postprocessing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.MessageBuilder;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.apache.james.mime4j.stream.Field;
import org.apache.james.mime4j.stream.MimeConfig;
import org.htmlcleaner.ContentNode;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.HtmlNode;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.TagNodeVisitor;

import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import com.pff.PSTObject;
import com.rtfparserkit.converter.text.StringTextConverter;
import com.rtfparserkit.parser.RtfStreamSource;
import com.rtfparserkit.rtf.Command;
import com.rtfparserkit.rtf.CommandType;

public abstract class EmailProcessor {

	private int numProcessed = 0;

	protected abstract void processEmlMessage(Message msg, String fileName) throws Exception;
	protected abstract void processPstMessage(PSTMessage msg, Header header, String parentFolderName, File f)
			throws Exception;

	private static final String[] separators = {
		"-----", "_____", "*****", "=====",
		"Sent from my", "Sent\u00a0from\u00a0my", "Inviato da", "Invio\u00a0eseguito\u00a0dallo",
		"Begin forwarded message:",
		"Milan Singapore Washington DC"	// Hacking Team signature
	};
	protected static final int[] separatorHisto = new int[separators.length];

	private static final Pattern citePrefixPattern = Pattern.compile("^((On)|(Il)).*,.*((wrote)|(ha scritto)):\\r?$");
	private static final String[] separatorRegexes = {
		"^\\p{Zs}*--\\p{Zs}*\\r?$",
		"^----? ?((Original Message)|(Messaggio originale)) ?----?\\r?$",
		"^\\p{Zs}*((From)|(Da)|(To)|(A))\\p{Zs}*:((\\p{Zs}+.*\\r?)|(\\r?$))",
		"^HT srl\u00a0?\\r?$",	// Hacking Team signature
		"^www\\.hackingteam\\.com\\r?$"	// Hacking Team signature
	};
	private static final Pattern[] separatorPatterns;
	static {
		separatorPatterns = new Pattern[separatorRegexes.length];
		for (int i = 0; i < separatorRegexes.length; i++)
			separatorPatterns[i] = Pattern.compile(separatorRegexes[i]);
	}

	protected static String cleanMessageBody(String body) {
		StringBuilder out = new StringBuilder();
		String[] lines = body.split("\\n");
nextLine:
		for (String line : lines) {
			int idx = 0;
			for (String sep : separators) {	// ignore all lines below a separator
				if (line.contains(sep)) {
					separatorHisto[idx]++;
					break nextLine;
				}
				idx++;
			}
			for (Pattern pat : separatorPatterns)
				if (pat.matcher(line).matches())
					break nextLine;
			if (line.startsWith(">") || citePrefixPattern.matcher(line).matches())	// ignore all quoted lines
				continue nextLine;

			// convert Windows to UNIX line breaks if necessary
			line.replace('\r', '\n');
			out.append(line);
			if (!line.endsWith("\n"))
				out.append('\n');
		}
		return out.toString();
	}

	protected static String stripHtmlTags(String html) {
		HtmlCleaner cleaner = new HtmlCleaner();
		cleaner.getProperties().setDeserializeEntities(true);
		TagNode node = cleaner.clean(html);

		// traverse DOM tree
		final StringBuilder sb = new StringBuilder();
		node.traverse(new TagNodeVisitor() {
			@Override
			public boolean visit(TagNode parentNode, HtmlNode htmlNode) {
				if (htmlNode instanceof TagNode) {
					TagNode tag = (TagNode) htmlNode;
					String tagName = tag.getName();
					if ((tagName.equals("blockquote") && "cite".equals(tag.getAttributeByName("type"))) ||
						(tagName.equals("pre") &&
								("moz-signature".equals(tag.getAttributeByName("class")) ||
								 "k9mail".equals(tag.getAttributeByName("class")))) ||
						(tagName.equals("div") &&
								("moz-signature".equals(tag.getAttributeByName("class")) ||
								 "moz-txt-sig".equals(tag.getAttributeByName("class")) ||
								 "gmail_quote".equals(tag.getAttributeByName("class")))) ||
						tagName.equals("style")) {
						// Keeping div with class "moz-cite-prefix", as sometimes actual content ends up within the div.
						// The expected generated content ("On ..., ... wrote:") will be removed in the plain text
						// cleanup stage. Same for "moz-forward-container", some clowns really like to write into it.
						tag.removeAllChildren();
					} else if (tagName.equals("div")) {
						// Outlook provides no semantic information about quoted content. Best we can do is detect the
						// first quoted message and stop the text extraction. Hopefully Outlook users don't bottom
						// post... Fortunately, the kind of person who insists on bottom posting usually sends plain
						// text mails anyway.
						String style = tag.getAttributeByName("style");
						if ((style != null) && style.contains("border-top:solid"))
							return false;
						sb.append('\n');
					} else if (tagName.equals("br") || tagName.equals("p") || tagName.equals("span")) {
						sb.append('\n');
					}
				} else if (htmlNode instanceof ContentNode) {
					sb.append(((ContentNode) htmlNode).getContent());
				}
				return true;
			}
		});
		return sb.toString();
	}

	protected static class ParsedRtf extends StringTextConverter {
		private Command currentDestination = Command.rtf;
		private final Deque<Boolean> groupHtmlrtfState = new ArrayDeque<Boolean>();
		private boolean inHtmlrtf = false;
		public boolean isEncapsulatedHtml = false;

		@Override
		public void processGroupStart() {
			groupHtmlrtfState.push(inHtmlrtf);
			super.processGroupStart();
		}

		@Override
		public void processGroupEnd() {
			inHtmlrtf = groupHtmlrtfState.pop();
			super.processGroupEnd();
		}

		@Override
		public void processCommand(Command command, int parameter, boolean hasParameter, boolean optional) {
			if (command.getCommandType() == CommandType.Destination)
				currentDestination = command;

			switch (command) {
			case fromhtml:
				if (hasParameter && (parameter == 1))
					isEncapsulatedHtml = true;
				break;
			case htmlrtf:
				inHtmlrtf = (!hasParameter || (parameter == 1));
				break;
			default:
				super.processCommand(command, parameter, hasParameter, optional);
			}
		}

		@Override
		public void processString(String string) {
			if (isEncapsulatedHtml && (currentDestination == Command.htmltag))
				processExtractedText(string);
			else
				super.processString(string);
		}

		@Override
		public void processExtractedText(String text) {
			if (!isEncapsulatedHtml || !inHtmlrtf)
				super.processExtractedText(text);
		}
	}

	/**
	 * Input may be either plain RTF or HTML encapsulated in RTF as specified in MS-OXRTFEX. If encapsulated HTML is
	 * present, the output is a HTML document, otherwise it is plain text.
	 */
	protected static ParsedRtf parseRtf(String rtf) throws IOException {
		ParsedRtf converter = new ParsedRtf();
		// This is rather ugly: libpst stores the RTF data in a String using the system's default encoding, so we
		// convert it back to a byte array using the default encoding, hoping to get the original data back.
		converter.convert(new RtfStreamSource(new ByteArrayInputStream(rtf.getBytes())));
		return converter;
	}

	protected static boolean hasReplyPrefix(String subject) {
		return (subject.toLowerCase().startsWith("re:") ||	// should catch "Re:" and "RE:"
				subject.startsWith("R:"));	// Italian
	}

	protected static boolean hasForwardPrefix(String subject) {
		return (subject.toLowerCase().startsWith("fw") ||	// should catch "Fwd", "FWD", "FW", and "Fw"
				subject.startsWith("I:"));	// Italian
	}

	private MessageBuilder createMessageBuilder() {
		MimeConfig.Builder config = new MimeConfig.Builder();
		config.setMaxHeaderLen(1024 * 1024);
		config.setMaxLineLen(64 * 1024);
		config.setMaxHeaderCount(16 * 1024);
		DefaultMessageBuilder builder = new DefaultMessageBuilder();
		builder.setMimeEntityConfig(config.build());
		return builder;
	}

	protected void processEmlFile(InputStream is, String fileName) throws IOException {
		MessageBuilder builder = createMessageBuilder();
		try {
			Message msg = builder.parseMessage(is);
			Field contentClass = msg.getHeader().getField("Content-Class");
			if ((contentClass == null) || contentClass.getBody().trim().equals("urn:content-classes:message"))
				processEmlMessage(msg, fileName);
		} catch (Exception ex) {
			throw new IOException("error while processing '" + fileName + "'", ex);
		} finally {
			is.close();
		}
	}

	protected void processPstFile(File f) throws IOException {
		MessageBuilder builder = createMessageBuilder();
		try {
			PSTFile pst = new PSTFile(f);
			Queue<PSTFolder> workQueue = new LinkedList<PSTFolder>();
			workQueue.add(pst.getRootFolder());
			while (!workQueue.isEmpty()) {
				PSTFolder folder = workQueue.poll();
				if (folder.hasSubfolders())
					workQueue.addAll(folder.getSubFolders());

				PSTObject obj = folder.getNextChild();
				while (obj != null) {
					String typeId = obj.getMessageClass();
					if (typeId.startsWith("IPM.Note")) {
						PSTMessage msg = (PSTMessage) obj;
						Header header = builder.parseHeader(new ByteArrayInputStream(
								msg.getTransportMessageHeaders().getBytes()));
						processPstMessage(msg, header, folder.getDisplayName(), f);
					}
					obj = folder.getNextChild();
				}
			}
		} catch (Exception ex) {
			throw new IOException("error while processing '" + f.getPath() + "'", ex);
		}
	}

	protected void processZipFile(File f) throws IOException {
		ZipFile archive = new ZipFile(f);
		try {
			Enumeration<? extends ZipEntry> e = archive.entries();
			while (e.hasMoreElements()) {
				ZipEntry entry = e.nextElement();
				if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".eml"))
					processEmlFile(archive.getInputStream(entry), f.getPath() + ":" + entry.getName());
			}
		} finally {
			archive.close();
		}
	}

	public void processFiles(File baseDir) throws IOException {
		processFiles(baseDir, Collections.<String>emptySet());
	}

	public void processFiles(File baseDir, Set<String> ignoredFiles) throws IOException {
		Queue<File> dirs = new LinkedList<File>();
		dirs.add(baseDir);
		while (!dirs.isEmpty()) {
			File curDir = dirs.poll();
			for (File f : curDir.listFiles()) {
				if (f.isDirectory()) {
					dirs.add(f);
				} else {
					String name = f.getName().toLowerCase();
					if (!ignoredFiles.contains(name)) {
						if (name.endsWith(".eml"))
							processEmlFile(new FileInputStream(f), f.getPath());
						else if (name.endsWith(".pst"))
							processPstFile(f);
						else if (name.endsWith(".zip"))
							processZipFile(f);
					}
					if ((++numProcessed % 10) == 0)
						System.err.println("processed " + numProcessed + " files");
				}
			}
		}
	}

}
