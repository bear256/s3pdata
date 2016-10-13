package s3p.ws.config;

public class Platform {

	public static final String Twitter = "twitter";
	public static final String MSDN = "msdn";
	public static final String TechNet = "tn";
	public static final String ServerFault = "sf";
	public static final String StackOverflow = "so";
	public static final String SuperUser = "su";

	public static String getWeeklyTableName(String platform) {
		switch (platform) {
		case Platform.Twitter:
			platform = "Twitter";
			break;
		case Platform.MSDN:
			platform = "MSDN";
			break;
		case Platform.TechNet:
			platform = "TechNet";
			break;
		case Platform.ServerFault:
			platform = "ServerFault";
			break;
		case Platform.StackOverflow:
			platform = "StackOverflow";
			break;
		case Platform.SuperUser:
			platform = "SuperUser";
			break;
		}
		return String.format("%sWeekly", platform);
	}

	public static String getDailyTableName(String platform) {
		switch (platform) {
		case Platform.Twitter:
			platform = "Twitter";
			break;
		case Platform.MSDN:
			platform = "MSDN";
			break;
		case Platform.TechNet:
			platform = "TechNet";
			break;
		case Platform.ServerFault:
			platform = "ServerFault";
			break;
		case Platform.StackOverflow:
			platform = "StackOverflow";
			break;
		case Platform.SuperUser:
			platform = "SuperUser";
			break;
		}
		return String.format("%sDaily", platform);
	}

	public static String getHourlyTableName(String platform) {
		switch (platform) {
		case Platform.Twitter:
			platform = "Twitter";
			break;
		case Platform.MSDN:
			platform = "MSDN";
			break;
		case Platform.TechNet:
			platform = "TechNet";
			break;
		case Platform.ServerFault:
			platform = "ServerFault";
			break;
		case Platform.StackOverflow:
			platform = "StackOverflow";
			break;
		case Platform.SuperUser:
			platform = "SuperUser";
			break;
		}
		return String.format("%sHourly", platform);
	}
}
