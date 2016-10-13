package s3p.data.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.alibaba.fastjson.JSON;

import s3p.data.endpoint.common.Sentiment;

public class KeywordsUtils {

	private static Map<String, List<String>> map = null;

	public static List<String> getAnotherWords(String word) throws IOException {
		List<String> list = new ArrayList<>();
		String url = String.format("http://www.wordhippo.com/what-is/another-word-for/%s.html",
				word.replaceAll(" ", "_"));
		Document doc = Jsoup.connect(url)
				.userAgent("Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko").get();
		Elements elements = doc.select("div.wordblock > a");
		for (Element element : elements) {
			String keyword = element.text();
			list.add(keyword);
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public static HashMap<String, List<String>> loadKeywords() {
		InputStream inStream = KeywordsUtils.class.getResourceAsStream("keywords/keywords.json");
		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return JSON.parseObject(sb.toString(), HashMap.class);
	}

	public static void init() {
		map = new HashMap<>();
		try {
			System.out.println("Load keywords from http://www.wordhippo.com/what-is/another-word-for/XXX.html");
			List<String> neg = getAnotherWords("not working");
			neg.add("service down");
			neg.add("down-graded");
			neg.add("cost a lot");
			neg.add("do not like it");
			map.put(Sentiment.NEG, neg);
			List<String> posi = getAnotherWords("good");
			posi.add("good");
			map.put(Sentiment.POSI, posi);
		} catch (Exception e) {
			System.out.println("Load keywords from local file");
			map.putAll(loadKeywords());
		}
	}

	public static List<String> match(String pn, String text) {
		if (map == null) {
			init();
		}
		List<String> keywords = new ArrayList<>();
		List<String> dict = map.get(pn);
		for (String keyword : dict) {
			if (text.toLowerCase().indexOf(keyword.toLowerCase()) >= 0) {
				keywords.add(keyword.toLowerCase());
			}
		}
		return keywords;
	}

	public static boolean isMatch(String text, String keyword) {
		return text.toLowerCase().indexOf(keyword.toLowerCase()) >= 0;
	}

	public static void main(String[] args) {
		// String text = "Azure service down";
		// List<String> keywords = match("NEG", text);
		// for(String keyword: keywords) {
		// System.out.println(keyword);
		// }
		// System.out.println("".compareTo("B:123"));
		init();
		System.out.println(JSON.toJSONString(map));
	}

}
