package s3p.data.endpoint.common.attachedobject;

public class MentionedMostServiceAttachedObject {

	private String name;
	private Long timeslot;

	public MentionedMostServiceAttachedObject() {
		super();
	}

	public MentionedMostServiceAttachedObject(String name, Long timeslot) {
		super();
		this.name = name;
		this.timeslot = timeslot;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getTimeslot() {
		return timeslot;
	}

	public void setTimeslot(Long timeslot) {
		this.timeslot = timeslot;
	}

}
