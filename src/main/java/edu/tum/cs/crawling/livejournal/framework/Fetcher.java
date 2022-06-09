package edu.tum.cs.crawling.livejournal.framework;

import java.net.URL;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class Fetcher {

	private static final int blockSize = 4096;
	private static final int timeout = 10;	// seconds

	private String userAgent;

	public Fetcher(String userAgent)
	{
		this.userAgent = userAgent;
	}

	public Resource retrieveUrl(String url) throws IOException
	{
		Resource res = new Resource(url);
		URL parsedUrl = new URL(url);

		HttpURLConnection conn = (HttpURLConnection) parsedUrl.openConnection();
		conn.setConnectTimeout(timeout * 1000);
		conn.setReadTimeout(timeout * 1000);
		conn.setRequestProperty("User-Agent", userAgent);
		conn.setAllowUserInteraction(false);
		conn.setInstanceFollowRedirects(false);
		conn.setUseCaches(false);
		conn.connect();

		// read header
		res.resultCode = conn.getResponseCode();
		String headerField;
		int i = 0;
		while ((headerField = conn.getHeaderField(i)) != null)
		{
			String headerKey = conn.getHeaderFieldKey(i);
			res.addHeader(headerKey, headerField);
			i++;
		}

		// read content
		if (res.resultCode == HttpURLConnection.HTTP_OK)
		{
			InputStream in = conn.getInputStream();
			int contentLength = conn.getContentLength();
			ByteArrayOutputStream buf;
			if (contentLength > 0)
				buf = new ByteArrayOutputStream(contentLength);
			else
				buf = new ByteArrayOutputStream();
			byte[] blockBuf = new byte[blockSize];
			int len;
			while ((len = in.read(blockBuf, 0, blockSize)) != -1)
				buf.write(blockBuf, 0, len);
			res.setContent(buf.toByteArray());
			in.close();
		}

		conn.disconnect();
		return res;
	}

}
