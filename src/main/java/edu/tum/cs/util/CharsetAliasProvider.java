package edu.tum.cs.util;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.spi.CharsetProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CharsetAliasProvider extends CharsetProvider {

	private static final String[][] aliases = {
		{ "ISO-8859-8-I", "ISO-8859-8" }
	};

	private final Map<String, Charset> charsetsByName;

	public CharsetAliasProvider() {
		charsetsByName = new HashMap<String, Charset>();
		for (String[] alias : aliases)
			charsetsByName.put(alias[0], createCharset(alias));
	}

	private Charset createCharset(String[] alias) {
		final Charset target = Charset.forName(alias[1]);
		return new Charset(alias[0], new String[0]) {
			@Override
			public boolean contains(Charset cs) {
				return target.contains(cs);
			}

			@Override
			public CharsetDecoder newDecoder() {
				return target.newDecoder();
			}

			@Override
			public CharsetEncoder newEncoder() {
				return target.newEncoder();
			}
		};
	}

	@Override
	public Charset charsetForName(String name) {
		return charsetsByName.get(name);
	}

	@Override
	public Iterator<Charset> charsets() {
		return charsetsByName.values().iterator();
	}

}
