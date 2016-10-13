package s3p.data.endpoint.common;

public class Sentiment {

	public static final int UNDEFINED = -1;
	public static final int NEGATIVE = 0;
	public static final int NEUTRAL = 2;
	public static final int POSITIVE = 4;
	public static final String ALL = "ALL";
	public static final String UNDEF = "UNDEF";
	public static final String NEG = "NEG";
	public static final String NEU = "NEU";
	public static final String POSI = "POSI";

	public static String get(int sentimentScore) {
		String s = "ALL";
		switch (sentimentScore) {
		case UNDEFINED:
			s = UNDEF;
			break;
		case NEGATIVE:
			s = NEG;
			break;
		case NEUTRAL:
			s = NEU;
			break;
		case POSITIVE:
			s = POSI;
			break;
		}
		return s;
	}
}
