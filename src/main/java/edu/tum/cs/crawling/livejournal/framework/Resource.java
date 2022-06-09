package edu.tum.cs.crawling.livejournal.framework;

import java.util.Date;
import java.lang.StringBuffer;

public class Resource {

	public static final int INVALID_RESPONSE = -1;

	public final Date timeStamp;
	public final String url;
	public StringBuffer header;
	public byte[] content;
	public String contentType;
	public int resultCode;

	public Resource(String url)
	{
		timeStamp = new Date();
		this.url = url;
		header = new StringBuffer();
		content = null;
		contentType = null;
	}

	public void addHeader(String key, String value)
	{
		if (key != null)
		{
			if (key.equalsIgnoreCase("Transfer-Encoding"))
				return;
			if (key.equalsIgnoreCase("Content-Type"))
				contentType = value;
			header.append(key);
			header.append(": ");
			header.append(value);
		}
		else
			header.append(value);	// status line
		header.append('\n');
	}

	public void setContent(byte[] buf)
	{
		content = buf;
	}

}
