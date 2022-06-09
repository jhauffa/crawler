package edu.tum.cs.crawling.livejournal.framework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.lang.StringBuffer;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;


public class WarcWriter {

	private static final String crlf = "\r\n";
	private static final String recordDelimiter = crlf + crlf;

	private FileOutputStream out;
	private String fileName;

	public WarcWriter(String prefix, String userAgent) throws IOException
	{
		// Construct file name according to the pattern given in Annex B of the
		// WARC specification.
		String timeStamp = getTimeStampGmt();
		String hostName = InetAddress.getLocalHost().getHostName();
		fileName = prefix + "-" + timeStamp + "-1-" + hostName + ".warc";

		// create new archive with specified prefix
		out = new FileOutputStream(fileName);
		writeInfoRecord(hostName, userAgent);
	}

	public WarcWriter(File file, String userAgent) throws IOException
	{
		// append to file, if already existing
		fileName = file.getName();
		boolean exists = file.exists();
		out = new FileOutputStream(file, true);
		if (!exists)
			writeInfoRecord(InetAddress.getLocalHost().getHostName(),
					userAgent);
	}

	private String generateUUID()
	{
		return "<urn:uuid:" + UUID.randomUUID().toString() + ">";
	}

	private void writeInfoRecord(String hostName, String userAgent)
		throws IOException
	{
		StringBuffer content = new StringBuffer();
		content.append("software: SocialWebCrawler" + crlf);
		content.append("hostname: " + hostName + crlf);
		content.append("http-header-user-agent: " + userAgent + crlf);
		content.append(
"format: WARC file version 0.18" + crlf +
"conformsTo: http://www.archive.org/documents/WarcFileFormat-0.18.html" + crlf);

		StringBuffer record = new StringBuffer();
		record.append("WARC/0.18" + crlf);
		record.append("WARC-Type: warcinfo" + crlf);
		record.append("WARC-Date: " + getTimeStampW3C() + crlf);
		record.append("WARC-Record-ID: " + generateUUID() + crlf);
		record.append("Content-Type: application/warc-fields" + crlf);
		record.append("Content-Length: " + Integer.toString(content.length()) +
				crlf + crlf);
		record.append(content);
		record.append(recordDelimiter);

		out.write(record.toString().getBytes("UTF-8"));
		out.flush();
	}

	public void write(Resource res) throws IOException
	{
		StringBuffer header = new StringBuffer();
		header.append("WARC/0.18" + crlf);
		header.append("WARC-Type: response" + crlf);
		header.append("WARC-Target-URI: " + res.url + crlf);
		header.append("WARC-Date: " + getTimeStampW3C() + crlf);
		header.append("WARC-Record-ID: " + generateUUID() + crlf);
		header.append("Content-Type: application/http;msgtype=response" + crlf);
		if (res.contentType != null)
			header.append("WARC-Identified-Payload-Type: " + res.contentType +
					crlf);
		int contentLength = res.header.length() + 1;
		if (res.content != null)
			contentLength += res.content.length;
		header.append("Content-Length: " + Integer.toString(contentLength) +
				crlf + crlf);
		header.append(res.header);
		header.append("\n");	// part of the HTTP server's response

		out.write(header.toString().getBytes("UTF-8"));
		if (res.content != null)
			out.write(res.content);
		out.write(recordDelimiter.getBytes("UTF-8"));
		out.flush();
	}

	public void close() throws IOException
	{
		out.close();
	}

	public String getFileName()
	{
		return fileName;
	}

	private static void padDateComponent(StringBuffer buf, int c)
	{
		if (c < 10)
			buf.append('0');
		buf.append(Integer.toString(c));
	}

	private static String getTimeStampGmt()
	{
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		StringBuffer buf = new StringBuffer(14);
		buf.append(Integer.toString(cal.get(Calendar.YEAR)));
		padDateComponent(buf, cal.get(Calendar.MONTH) + 1);
		padDateComponent(buf, cal.get(Calendar.DAY_OF_MONTH));
		padDateComponent(buf, cal.get(Calendar.HOUR_OF_DAY));
		padDateComponent(buf, cal.get(Calendar.MINUTE));
		padDateComponent(buf, cal.get(Calendar.SECOND));
		return buf.toString();
	}

	private static String getTimeStampW3C()
	{
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		StringBuffer buf = new StringBuffer(20);
		buf.append(Integer.toString(cal.get(Calendar.YEAR)));
		buf.append('-');
		padDateComponent(buf, cal.get(Calendar.MONTH) + 1);
		buf.append('-');
		padDateComponent(buf, cal.get(Calendar.DAY_OF_MONTH));
		buf.append('T');
		padDateComponent(buf, cal.get(Calendar.HOUR_OF_DAY));
		buf.append(':');
		padDateComponent(buf, cal.get(Calendar.MINUTE));
		buf.append(':');
		padDateComponent(buf, cal.get(Calendar.SECOND));
		buf.append('Z');
		return buf.toString();
	}

}
