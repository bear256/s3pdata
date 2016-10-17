package s3p.data.endpoint.impactsummary;

import s3p.data.endpoint.mentionedmost.MentionedMostService;

public class MostDislikedService {
	private MentionedMostService mentionedmostservice;
	private Integer occupyratio;

	public MostDislikedService() {
		super();
		this.mentionedmostservice = new MentionedMostService();
		this.occupyratio = 0;
	}

	public MostDislikedService(MentionedMostService mentionedmostservice, Integer occupyratio) {
		super();
		this.mentionedmostservice = mentionedmostservice;
		this.occupyratio = occupyratio;
	}

	public MentionedMostService getMentionedmostservice() {
		return mentionedmostservice;
	}

	public void setMentionedmostservice(MentionedMostService mentionedmostservice) {
		this.mentionedmostservice = mentionedmostservice;
	}

	public Integer getOccupyratio() {
		return occupyratio;
	}

	public void setOccupyratio(Integer occupyratio) {
		this.occupyratio = occupyratio;
	}

}
