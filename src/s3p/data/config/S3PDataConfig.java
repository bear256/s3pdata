package s3p.data.config;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class S3PDataConfig {
	
	private Properties props;
	
	public S3PDataConfig() {
		InputStream inStream = S3PDataConfig.class.getClassLoader().getResourceAsStream("s3pdata.properties");
		props = new Properties();
		try {
			props.load(inStream);;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public S3PDataConfig(InputStream inStream) {
		props = new Properties();
		try {
			props.load(inStream);;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public S3PDataConfig(String filename) {
		props = new Properties();
		try {
			props.load(new FileInputStream(filename));;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String get(String key) {
		return props.getProperty(key);
	}
	
	public String get(String key, String defaultValue) {
		return props.getProperty(key, defaultValue);
	}

	public static void main(String[] args) {
		S3PDataConfig config = new S3PDataConfig();
		System.out.println(config.get("documentdb.uri"));
		System.out.println(config.get("documentdb.key"));
		System.out.println(config.get("storage.account"));
		System.out.println(config.get("storage.key"));
	}

}
