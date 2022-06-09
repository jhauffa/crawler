package edu.tum.cs.crawling.facebook.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;

public class WarcArchiver {

	private static final Logger logger = Logger.getLogger(WarcArchiver.class.getName());
	private static WarcArchiver instance;

	public static class Archive {
		public final String userId;
		public final Date date;
		public final Map<String, String> pageSource;

		public Archive(String userId, Date date, Map<String, String> pageSource) {
			this.userId = userId;
			this.date = date;
			this.pageSource = pageSource;
		}
	}

	private WarcArchiver() {
		try {
			Connection c = FacebookDao.getConnection();
			try {
				// create table "user_archive" if it does not yet exist
				Statement s = c.createStatement();
				s.executeUpdate("CREATE TABLE IF NOT EXISTS `user_archive` (" +
						"`user_id` VARCHAR(255) NOT NULL, `archive_key` CHAR(2) NOT NULL, " +
						"`is_processed` BIT NOT NULL, `received_at` TIMESTAMP NOT NULL, PRIMARY KEY (`user_id`)) " +
						"CHARACTER SET latin1 COLLATE latin1_bin");
			} finally {
				c.close();
			}
		} catch (SQLException ex) {
			logger.log(Level.SEVERE, "error preparing archive tables in database", ex);
		}
	}

	public File savePageSource(String userId, Map<String, String> pageSource) {
		return savePageSource(new Archive(userId, new Date(), pageSource));
	}

	public File savePageSource(Archive archive) {
		String archiveKey = String.format("%02X", archive.userId.hashCode() & 0xff);
		File targetDir = new File("archive", archiveKey);
		if (!targetDir.exists()) {
			if (!targetDir.mkdirs()) {
				logger.severe("error creating target directory '" + targetDir.getAbsolutePath() + "'");
				return null;
			}
		}

		File archiveFile = new File(targetDir, archive.userId + ".warc.gz");
		try {
			OutputStream os = new FileOutputStream(archiveFile);
			WarcWriter writer = WarcWriterFactory.getWriterCompressed(os);
			try {
				for (Map.Entry<String, String> e : archive.pageSource.entrySet()) {
					// prepare payload: fake HTTP response header to store character encoding
					byte[] responseContent = e.getValue().getBytes("UTF-8");
					StringBuilder sb = new StringBuilder();
					sb.append("HTTP/1.1 200 OK\r\n");
					sb.append("Content-Type: text/html; charset=UTF-8\r\n");
					sb.append("Content-Length: " + responseContent.length + "\r\n");
					sb.append("\r\n");
					byte[] responseHeader = sb.toString().getBytes("US-ASCII");
					byte[] payload = new byte[responseHeader.length + responseContent.length];
					System.arraycopy(responseHeader, 0, payload, 0, responseHeader.length);
					System.arraycopy(responseContent, 0, payload, responseHeader.length, responseContent.length);

					// build WARC record header
					WarcRecord record = WarcRecord.createRecord(writer);
					record.header.addHeader(WarcConstants.FN_WARC_RECORD_ID, "urn:uuid:" + UUID.randomUUID());
					record.header.addHeader(WarcConstants.FN_WARC_DATE, archive.date, null);
					record.header.addHeader(WarcConstants.FN_WARC_TYPE, WarcConstants.RT_RESPONSE);
					record.header.addHeader(WarcConstants.FN_WARC_TARGET_URI, e.getKey());
					record.header.addHeader(WarcConstants.FN_CONTENT_TYPE, "application/http; msgtype=response");
					record.header.addHeader(WarcConstants.FN_CONTENT_LENGTH, payload.length, null);

					// write record header and payload
					writer.writeHeader(record);
					writer.writePayload(payload);
					writer.closeRecord();
				}
			} finally {
				writer.close();
			}
		} catch (IOException ex) {
			logger.log(Level.SEVERE, "error writing WARC file", ex);
			return null;
		}

		try {
			Connection c = FacebookDao.getConnection();
			try {
				PreparedStatement psInsertUserId = c.prepareStatement("INSERT INTO user_archive (user_id,archive_key," +
						"is_processed,received_at) VALUES (?,?,0,?) ON DUPLICATE KEY UPDATE user_id = user_id");
				psInsertUserId.setString(1, archive.userId);
				psInsertUserId.setString(2, archiveKey);
				psInsertUserId.setTimestamp(3, new Timestamp(archive.date.getTime()));
				psInsertUserId.executeUpdate();
			} finally {
				c.close();
			}
		} catch (SQLException ex) {
			logger.log(Level.SEVERE, "error storing archive key in database", ex);
		}
		return archiveFile;
	}

	public static Archive loadPageSource(File archiveFile) throws IOException {
		Map<String, String> pageSource = new HashMap<String, String>();
		Date archiveDate = null;
		WarcReader reader = WarcReaderFactory.getReaderCompressed(new FileInputStream(archiveFile));
		try {
			WarcRecord record;
			while ((record = reader.getNextRecord()) != null) {
				String payload;
				InputStream is = record.getPayloadContent();
				try {
					payload = IOUtils.toString(is, "UTF-8");
				} finally {
					is.close();
				}
				pageSource.put(record.header.warcTargetUriStr, payload);
				archiveDate = record.header.warcDate;
			}
		} finally {
			reader.close();
		}

		String fileName = archiveFile.getName();
		String userId = fileName.substring(0, fileName.indexOf(".warc.gz"));
		return new Archive(userId, archiveDate, pageSource);
	}

	public synchronized void setIdProcessed(String userId) throws SQLException {
		Connection c = FacebookDao.getConnection();
		try {
			PreparedStatement psUpdateUserIdProcessed = c.prepareStatement(
					"UPDATE LOW_PRIORITY user_archive SET is_processed = 1 WHERE user_id = ?");
			psUpdateUserIdProcessed.setString(1, userId);
			psUpdateUserIdProcessed.executeUpdate();
		} finally {
			c.close();
		}
	}

	public List<File> getUnprocessedArchives() {
		List<File> archiveFiles = new ArrayList<File>();
		try {
			Connection c = FacebookDao.getConnection();
			try {
				PreparedStatement psGetArchive = c.prepareStatement("SELECT user_id,archive_key FROM user_archive " +
						"WHERE is_processed = 0");
				ResultSet rs = psGetArchive.executeQuery();
				while (rs.next()) {
					File targetDir = new File("archive", rs.getString(2));
					archiveFiles.add(new File(targetDir, rs.getString(1) + ".warc.gz"));
				}
			} finally {
				c.close();
			}
		} catch (SQLException ex) {
			logger.log(Level.WARNING, "error retrieving unprocessed archives from database", ex);
		}
		return archiveFiles;
	}

	public static synchronized WarcArchiver getInstance() {
		if (instance == null)
			instance = new WarcArchiver();
		return instance;
	}

}
