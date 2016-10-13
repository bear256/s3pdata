package s3p.data.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class ServiceNameUtils {

	private static Map<String, Map<String, String[]>> map = new HashMap<>();

	public static void init(String topic) {
		Map<String, String[]> dict = new HashMap<>();
		map.put(topic, dict);
		InputStream inputStream = ServiceNameUtils.class.getResourceAsStream("servicenames/" + topic + ".properties");
		if (inputStream != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String line = null;
			try {
				while ((line = br.readLine()) != null) {
					String[] fields = line.toLowerCase().replaceAll(":|,|and\\ |microsoft\\ |azure\\ ", "").split(" ");
					dict.put(line, fields);
					// System.out.println(StringUtils.join(fields, ","));
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static List<String> match4Twitter(String topic, String text) {
		List<String> servicenames = new ArrayList<>();
		if (!map.containsKey(topic.toUpperCase())) {
			init(topic.toUpperCase());
		}
		Map<String, String[]> dict = map.get(topic.toUpperCase());
		for (String servicename : dict.keySet()) {
			String[] fields = dict.get(servicename);
			int count = 0;
			for (String field : fields) {
				StringTokenizer tokenizer = new StringTokenizer(text.toLowerCase());
				while (tokenizer.hasMoreTokens()) {
					if (tokenizer.nextToken().startsWith(field)) {
						count++;
					}
				}
			}
			if (count > 0) {
				servicenames.add(servicename);
			}
		}
		return servicenames;
	}

	public static boolean isMatch4Twitter(String text, String serviceName) {
		String[] fields = serviceName.toLowerCase().replaceAll(":|,|and\\ |microsoft\\ |azure\\ ", "").split(" ");
		int count = 0;
		for (String field : fields) {
			StringTokenizer tokenizer = new StringTokenizer(text.toLowerCase());
			while (tokenizer.hasMoreTokens()) {
				if (tokenizer.nextToken().startsWith(field)) {
					count++;
				}
			}
		}
		return count > 0;
	}

	public static List<String> match4MSForum(String topic, String text) {
		List<String> servicenames = new ArrayList<>();
		if (!map.containsKey(topic.toUpperCase())) {
			init(topic.toUpperCase());
		}
		Map<String, String[]> dict = map.get(topic.toUpperCase());
		for (String servicename : dict.keySet()) {
			if (text.toLowerCase().indexOf(servicename.toLowerCase()) >= 0) {
				servicenames.add(servicename);
			}
		}
		return servicenames;
	}

	public static boolean isMatch4MSForum(String text, String serviceName) {
		return text.toLowerCase().indexOf(serviceName.toLowerCase()) >= 0;
	}

	public static List<String> match4StackForum(String topic, String text) {
		List<String> servicenames = new ArrayList<>();
		if (!map.containsKey(topic.toUpperCase())) {
			init(topic.toUpperCase());
		}
		Map<String, String[]> dict = map.get(topic.toUpperCase());
		for (String servicename : dict.keySet()) {
			String[] fields = dict.get(servicename);
			int count = 0;
			for (String field : fields) {
				StringTokenizer tokenizer = new StringTokenizer(text.toLowerCase());
				while (tokenizer.hasMoreTokens()) {
					if (tokenizer.nextToken().equals(field)) {
						count++;
					}
				}
			}
			if (count > 0) {
				servicenames.add(servicename);
			}
		}
		return servicenames;
	}

	public static boolean isMatch4StackForum(String text, String serviceName) {
		String[] fields = serviceName.toLowerCase().replaceAll(":|,|and\\ |microsoft\\ |azure\\ ", "").split(" ");
		int count = 0;
		for (String field : fields) {
			StringTokenizer tokenizer = new StringTokenizer(text.toLowerCase());
			while (tokenizer.hasMoreTokens()) {
				if (tokenizer.nextToken().equals(field)) {
					count++;
				}
			}
		}
		return count > 0;
	}

	public static void main(String[] args) throws IOException {
		String topic = "Azure";
		String text = "<p>After the update of VSTS (VSO) yesterday our Visual Studio Pre Build Events fails.</p>\n\n<p>We're doing a file copy from the source folder to the build folder of VSTS. For some reason after yesterdays update we now get an Access Denied error and the build fails.</p>\n\n<pre><code>copy \"$(ProjectDir)Web.config\" \"$(TargetDir)$(TargetFileName).config\"\n</code></pre>\n\n<blockquote>\n  <p>copy \"C:\\a\\1\\s\\Source\\ProjectName\\ProjectName\\Web.config\"\n  \"C:\\a\\1\\s\\Source\\ProjectName\\ProjectName\\bin\\ProjectName.dll.config\"\n  2016-08-18T06:10:46.9209275Z   Access is denied.\n  2016-08-18T06:10:46.9209275Z           0 file(s) copied.</p>\n</blockquote>\n\n<p>(The reason we copy Web.config is to get around an issue with Assembly redirects and Azure Web Roles.)</p>\n\n<p>The same problem occurs when we do file copies in gulp tasks into the same target directory.</p>\n\n<p>We use the Hosted Build Agents.</p>\n\n<p>Anyone know how to get around this issue?</p>\n\n<p><a href=\"https://www.visualstudio.com/en-us/news/2016-aug-17-vso\" rel=\"nofollow\">VSTS update 17 Aug</a></p>\n";
		// List<String> servicenames = ServiceNameUtils.match4MSForum(topic,
		// text);
		String[] tags = new String[] { "visual-studio", "azure-web", "vs-team-services", "vso-build" };
		List<String> servicenames = ServiceNameUtils.match4StackForum(topic, text);
		System.out.println(servicenames.isEmpty());
		for (String sn : servicenames) {
			System.out.println(sn);
		}
	}

}
