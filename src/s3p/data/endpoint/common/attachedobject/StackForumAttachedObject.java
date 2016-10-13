package s3p.data.endpoint.common.attachedobject;

import org.msgpack.annotation.Message;

import s3p.data.documentdb.stackforum.Owner;

@Message
public class StackForumAttachedObject {

	private Integer age;
	private String display_name;
	private String link;
	private String profile_image;
	private Integer reputation;
	private Long user_id;
	private String user_type;
	private Integer accept_rate;

	public StackForumAttachedObject() {
		super();
		// TODO Auto-generated constructor stub
	}

	public StackForumAttachedObject(Owner owner) {
		this.age = owner.getAge();
		this.display_name = owner.getDisplay_name();
		this.link = owner.getLink();
		this.profile_image = owner.getProfile_image();
		this.reputation = owner.getReputation();
		this.user_id = owner.getUser_id();
		this.user_type = owner.getUser_type();
		this.accept_rate = owner.getAccept_rate();
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getDisplay_name() {
		return display_name;
	}

	public void setDisplay_name(String display_name) {
		this.display_name = display_name;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getProfile_image() {
		return profile_image;
	}

	public void setProfile_image(String profile_image) {
		this.profile_image = profile_image;
	}

	public Integer getReputation() {
		return reputation;
	}

	public void setReputation(Integer reputation) {
		this.reputation = reputation;
	}

	public Long getUser_id() {
		return user_id;
	}

	public void setUser_id(Long user_id) {
		this.user_id = user_id;
	}

	public String getUser_type() {
		return user_type;
	}

	public void setUser_type(String user_type) {
		this.user_type = user_type;
	}

	public Integer getAccept_rate() {
		return accept_rate;
	}

	public void setAccept_rate(Integer accept_rate) {
		this.accept_rate = accept_rate;
	}
}
