package s3p.data.endpoint.mentionedmost;

import s3p.data.endpoint.common.VocInfluence;

public class MentionedMostServiceTrend {

	private String attachedobject;
	private long timeslot;
	private VocInfluence vocinfluence;

	public MentionedMostServiceTrend() {
		super();
	}

	public MentionedMostServiceTrend(String attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public String getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(String attachedobject) {
		this.attachedobject = attachedobject;
	}

	public long getTimeslot() {
		return timeslot;
	}

	public void setTimeslot(long timeslot) {
		this.timeslot = timeslot;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

}
