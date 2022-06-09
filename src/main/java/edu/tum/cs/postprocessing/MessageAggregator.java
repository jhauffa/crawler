package edu.tum.cs.postprocessing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MessageAggregator {

	private static class MessageContent {
		private final SocialMediaMessage message;
		private final String content;

		public MessageContent(SocialMediaMessage message, boolean ignoreWhitespace) {
			this.message = message;
			if (ignoreWhitespace)
				content = stripWhitespace(message.getContent());
			else
				content = message.getContent();
		}

		private static String stripWhitespace(String s) {
			StringBuilder sb = new StringBuilder(s.length());
			for (int i = 0; i < s.length(); i++) {
				char c = s.charAt(i);
				if (!Character.isWhitespace(c))
					sb.append(c);
			}
			return sb.toString();
		}

		@Override
		public int hashCode() {
			int result = 31 * 1 + content.hashCode();
			result = 31 * result + message.getDate().hashCode();
			return 31 * result + message.getSender().hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof MessageContent))
				return false;

			MessageContent other = (MessageContent) o;
			if (!other.content.equals(content))
				return false;
			SocialMediaMessage otherMessage = other.message;
			if (!otherMessage.getDate().equals(message.getDate()))
				return false;
			if (!otherMessage.getSender().equals(message.getSender()))
				return false;
			return true;
		}
	}

	private final boolean ignoreWhitespace;
	private final Map<String, SocialMediaMessage> messagesById = new HashMap<String, SocialMediaMessage>();
	private final Map<MessageContent, SocialMediaMessage> messagesByContent =
			new HashMap<MessageContent, SocialMediaMessage>();
	private int numSeen = 0;
	private int numEmpty = 0;
	private int numMerged = 0;
	private int numMergedById = 0;
	private int numDuplicate = 0;

	public MessageAggregator(boolean ignoreWhitespace) {
		this.ignoreWhitespace = ignoreWhitespace;
	}

	public SocialMediaMessage addMessage(SocialMediaMessage message) {
		return addMessage(message, false);
	}

	public SocialMediaMessage addMessage(SocialMediaMessage message, boolean keepEmptyMessage) {
		SocialMediaMessage target = null;

		numSeen++;
		if (keepEmptyMessage || !message.getContent().isEmpty()) {
			target = messagesById.get(message.getId());
			if (target == null) {
				MessageContent contentRef = new MessageContent(message, ignoreWhitespace);
				target = messagesByContent.get(contentRef);
				if (target != null) {
					if (target.getRecipients().addAll(message.getRecipients()))
						numMerged++;
					else
						numDuplicate++;
				} else {
					messagesByContent.put(contentRef, message);
					target = message;
				}
			} else {
				if (target.getRecipients().addAll(message.getRecipients()))
					numMergedById++;
				else
					numDuplicate++;
			}
			if (keepEmptyMessage && message.getContent().isEmpty())
				numEmpty++;
		} else {
			numEmpty++;
		}

		if (target != null)	// can only be null if message was discarded for being empty
			messagesById.put(message.getId(), target);
		return target;
	}

	public Collection<SocialMediaMessage> getMessages() {
		return messagesByContent.values();
	}

	public SocialMediaMessage getMessageById(String id) {
		return messagesById.get(id);
	}

	public int getNumEmpty() {
		return numEmpty;
	}

	public int getNumMerged() {
		return numMerged;
	}

	public int getNumDuplicate() {
		return numDuplicate;
	}

	@Override
	public String toString() {
		return numSeen + " messages, " +
				numEmpty + " (" + (((double) numEmpty / numSeen) * 100) + "%) empty, " +
				numMerged + " (" + (((double) numMerged / numSeen) * 100) + "%) merged, " +
				numMergedById + " (" + (((double) numMergedById / numSeen) * 100) + "%) merged (same ID), " +
				numDuplicate + " (" + (((double) numDuplicate / numSeen) * 100) + "%) discarded as duplicate";
	}

}
