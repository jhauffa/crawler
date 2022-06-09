package edu.tum.cs.crawling.facebook.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ClientRequestObject implements Serializable {

	public enum RequestType {
		UNKNOWN, REQUEST_ID, REQUEST_STATISTICS, DELIVER_PROFILE, REPORT_SCRAPE_ERROR
	}

	private static final long serialVersionUID = -2622010932122952627L;
	private static final Logger logger = Logger.getLogger(ClientRequestObject.class.getName());

	private RequestType requestType = RequestType.UNKNOWN;
	private String userId = null;
	private Map<String, String> pageSource = null;

	public RequestType getRequestType() {
		return requestType;
	}

	public void setRequestType(RequestType requestType) {
		this.requestType = requestType;
	}

	public Map<String, String> getPageSource() {
		return pageSource;
	}

	public void setPageSource(Map<String, String> pageSource) {
		this.pageSource = pageSource;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public static byte[] compress(ClientRequestObject cro) {
		byte[] data = null;
		try {
			ByteArrayOutputStream buf = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(new GZIPOutputStream(buf));
			os.writeObject(cro);
			os.close();
			data = buf.toByteArray();
		} catch (IOException ex) {
			logger.log(Level.SEVERE, "error compressing ClientRequestObject", ex);
		}
		return data;
	}

	public static ClientRequestObject decompress(byte[] data) {
		ClientRequestObject cro = null;
		try {
			ObjectInputStream is = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(data)));
			cro = (ClientRequestObject) is.readObject();
			is.close();
		} catch (Exception ex) {
			logger.log(Level.SEVERE, "error decompressing ClientRequestObject", ex);
		}
		return cro;
	}

}
