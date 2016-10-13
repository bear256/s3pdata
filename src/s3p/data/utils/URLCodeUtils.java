package s3p.data.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class URLCodeUtils {

	public static String encode(String str) {
		String encodeStr = null;
		try {
			encodeStr = URLEncoder.encode(""+str, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			System.out.println(str);
			e.printStackTrace();
		}
		return encodeStr;
	}
	
	public static String decode(String str) {
		String decodeStr = null;
		try {
			decodeStr = URLDecoder.decode(str, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return decodeStr;
	}
}
