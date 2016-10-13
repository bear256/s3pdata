package s3p.data.endpoint.dailyinfluence;

import s3p.data.endpoint.common.VocInfluence;

public class DailyInfluence {

	private Long attachedobject;
	private VocInfluence vocinfluence;

	public DailyInfluence() {
		super();
		this.vocinfluence = new VocInfluence();
	}

	public DailyInfluence(Long attachedobject) {
		super();
		this.attachedobject = attachedobject;
		this.vocinfluence = new VocInfluence();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		vocinfluence.incVocInfluence(sentiment, influenceCount);
	}

	public Long getAttachedobject() {
		return attachedobject;
	}

	public void setAttachedobject(Long attachedobject) {
		this.attachedobject = attachedobject;
	}

	public VocInfluence getVocinfluence() {
		return vocinfluence;
	}

	public void setVocinfluence(VocInfluence vocinfluence) {
		this.vocinfluence = vocinfluence;
	}

}
