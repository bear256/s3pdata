package s3p.data.documentdb.twitter;

import org.msgpack.annotation.Message;

@Message
public class Entities {

	private Url[] urls;
	private UserMetion[] userMetions;

	public Entities() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Url[] getUrls() {
		return urls;
	}

	public void setUrls(Url[] urls) {
		this.urls = urls;
	}

	public UserMetion[] getUserMetions() {
		return userMetions;
	}

	public void setUserMetions(UserMetion[] userMetions) {
		this.userMetions = userMetions;
	}
}
