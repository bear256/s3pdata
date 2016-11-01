package s3p.data.endpoint.mentionedmost;

import s3p.data.endpoint.common.VocInfluence;
import s3p.data.endpoint.common.attachedobject.MentionedMostServiceAttachedObject;

public class MentionedMostServiceTrend {

	private MentionedMostServiceAttachedObject attachedobject;
	private VocInfluence vocinfluence;

	public MentionedMostServiceTrend() {
		super();
	}
	
	public MentionedMostServiceTrend(MentionedMostService mentionedMostService, long timeslot) {
		super();
		this.attachedobject = new MentionedMostServiceAttachedObject(mentionedMostService.getAttachedobject(), timeslot);
		this.vocinfluence = mentionedMostService.getVocinfluence();
	}

	public MentionedMostServiceTrend(MentionedMostServiceAttachedObject attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public MentionedMostServiceAttachedObject getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(MentionedMostServiceAttachedObject attachedobject) {
		this.attachedobject = attachedobject;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

}
