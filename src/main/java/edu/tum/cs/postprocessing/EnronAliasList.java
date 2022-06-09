package edu.tum.cs.postprocessing;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class EnronAliasList extends UserAliasList {

	@Override
	public String normalize(String name) {
		name = name.toLowerCase();

		// remove excessive host names
		int endPos = name.indexOf('@');
		if (endPos >= 0) {
			endPos = name.indexOf('@', endPos + 1);
			if (endPos >= 0)
				name = name.substring(0, endPos);
		}

		// processing of Enron-internal addresses
		if (name.endsWith("@enron.com") || name.endsWith("@enron.net") ||
			name.endsWith("@ect.enron.com") || name.endsWith("@ei.enron.com") || name.endsWith("@ees.enron.com") ||
			name.endsWith("@mailman.enron.com") || name.endsWith("@exchange.enron.com")) {
			name = name.replace("'", "").replace("_", ".").replace("..", ".");
			name = name.substring(0, name.indexOf('@')) + "@enron";	// normalize domain name
		}
		return name;
	}

	public static String untransformAddress(String address) {
		if (!address.toLowerCase().startsWith("imceanotes-"))
			return address;

		// undo "IMCEANOTES" transformation: strip "@enron" suffix if present, fix URL encoding, decode
		int endPos = address.indexOf('@');
		if (endPos < 0)
			endPos = address.length();
		address = address.substring(11, endPos).replace('+', '%');
		try {
			address = URLDecoder.decode(address, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException("UTF-8 encoding not supported, should never happen", ex);
		}

		// encoded string is either a simple email address or includes a display name
		int addrStartIdx = address.indexOf('<');
		int addrEndIdx = address.indexOf('>', addrStartIdx + 1);
		if ((addrStartIdx >= 0) && (addrEndIdx >= 0)) {
			address = address.substring(addrStartIdx + 1, addrEndIdx);
		} else {	// address not specified or truncated; display name may be a valid address
			addrStartIdx = address.indexOf('"');
			addrEndIdx = address.indexOf('"', addrStartIdx + 1);
			if ((addrStartIdx >= 0) && (addrEndIdx >= 0))
				address = address.substring(addrStartIdx + 1, addrEndIdx);
		}
		return address;
	}

}
