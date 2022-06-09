package edu.tum.cs.crawling.twitter.entities;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.AttributeConverter;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Converter;
import javax.persistence.Entity;
import javax.persistence.Table;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.ColumnTransformer;

@Entity
@Table(name="USER")
@Cacheable
@Cache(usage=CacheConcurrencyStrategy.READ_WRITE)
public class FollowersFriends extends TwitterUserData {

	private static final long serialVersionUID = 906661713587248277L;

	@Converter
	public static class HashSetSerializer implements AttributeConverter<HashSet<?>, byte[]> {
		@Override
		public byte[] convertToDatabaseColumn(HashSet<?> attribute) {
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(attribute);
				oos.close();
				return bos.toByteArray();
			} catch (IOException ex) {
				throw new RuntimeException("error serializing object", ex);
			}
		}

		@Override
		public HashSet<?> convertToEntityAttribute(byte[] dbData) {
			throw new RuntimeException("de-serialization not implemented");
		}
	}

	@Column(name="FOLLOWERS_SER", length=16777215)	// maps to MySQL MEDIUMBLOB
	@ColumnTransformer(read = "UNCOMPRESS(FOLLOWERS_SER)", write = "COMPRESS(?)")
	@Convert(converter = HashSetSerializer.class)
	private Set<Long> followers = new HashSet<Long>();

	@Column(name="FRIENDS_SER", length=16777215)	// maps to MySQL MEDIUMBLOB
	@ColumnTransformer(read = "UNCOMPRESS(FRIENDS_SER)", write = "COMPRESS(?)")
	@Convert(converter = HashSetSerializer.class)
	private Set<Long> friends = new HashSet<Long>();

	/**
	 * @return the crawled followers
	 */
	public Set<Long> getFollowers() {
		return followers;
	}

	/**
	 * @return the crawled friends
	 */
	public Set<Long> getFriends() {
		return friends;
	}

	public void addFriends(long[] ids) {
		for (long id : ids) {
			friends.add(id);
		}
	}

	public void addFollowers(long[] ids) {
		for (long id : ids) {
			followers.add(id);
		}
	}

}
