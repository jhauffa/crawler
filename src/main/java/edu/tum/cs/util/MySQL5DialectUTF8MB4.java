package edu.tum.cs.util;

import org.hibernate.dialect.MySQL5Dialect;

public class MySQL5DialectUTF8MB4 extends MySQL5Dialect {

	@Override
	public String getTableTypeString() {
		return " DEFAULT CHARSET=utf8mb4";
	}

}
