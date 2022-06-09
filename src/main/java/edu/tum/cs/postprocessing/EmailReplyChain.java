package edu.tum.cs.postprocessing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

public class EmailReplyChain {

	private static class ThreadIndex {
		private final byte[] index;

		public ThreadIndex(byte[] index) {
			this.index = index;
		}

		public ThreadIndex(String s) {
			index = Base64.decodeBase64(s);
		}

		public ThreadIndex getParent() {
			if (index.length <= 22)
				return null;
			return new ThreadIndex(Arrays.copyOfRange(index, 0, index.length - 5));
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(index);
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ThreadIndex))
				return false;
			return Arrays.equals(index, ((ThreadIndex) obj).index);
		}
	}

	private final Map<String, String> replyChain = new HashMap<String, String>();
	private final Map<ThreadIndex, String> threadToMessageId = new HashMap<ThreadIndex, String>();

	public void addMessageId(String messageId, String parentMessageId) {
		if (!parentMessageId.equals(messageId))
			replyChain.put(messageId, parentMessageId);
	}

	public void addThreadIndex(String threadIndex, String messageId) {
		threadToMessageId.put(new ThreadIndex(threadIndex), messageId);
	}

	/** merge Microsoft's special reply chain with regular reply chain */
	private void mergeThreadIndices() {
		for (Map.Entry<ThreadIndex, String> e : threadToMessageId.entrySet()) {
			ThreadIndex parentThreadId = e.getKey().getParent();
			if (parentThreadId != null) {	// is reply to a previous message
				String parentMessageId = threadToMessageId.get(parentThreadId);
				if (parentMessageId != null)
					addMessageId(e.getValue(), parentMessageId);
			}
		}
	}

	public Map<String, String> getReplyChain() {
		mergeThreadIndices();
		return replyChain;
	}

}
