package s3p.data.endpoint.impactsummary;

import s3p.data.endpoint.regiondistribution.RegionDistribution;

public class ImpactSummary {

	private InfluenceOfUsers influenceofusers;
	private JoinedUsers joinedusers;
	private MentionedServiceCount mentionedservicecount;
	private MostDislikedService[] mostdislikedservice;
	private MostLikedService[] mostlikedservice;
	private RegionDistribution mostnegfrom;
	private RegionDistribution mostposifrom;
	private RegionOfUsers regionofusers;
	private VocInsights vocinsights;

	public ImpactSummary() {
		super();
		this.influenceofusers = new InfluenceOfUsers();
		this.joinedusers = new JoinedUsers();
		this.mentionedservicecount = new MentionedServiceCount();
		this.mostdislikedservice = new MostDislikedService[0];
		this.mostlikedservice = new MostLikedService[0];
		this.mostnegfrom = new RegionDistribution();
		this.mostposifrom = new RegionDistribution();
		this.regionofusers = new RegionOfUsers();
		this.vocinsights = new VocInsights();
	}

	public ImpactSummary(InfluenceOfUsers influenceofusers, JoinedUsers joinedusers,
			MentionedServiceCount mentionedservicecount, MostDislikedService[] mostdislikedservice,
			MostLikedService[] mostlikedservice, RegionDistribution mostnegfrom, RegionDistribution mostposifrom,
			RegionOfUsers regionofusers, VocInsights vocinsights) {
		super();
		this.influenceofusers = influenceofusers;
		this.joinedusers = joinedusers;
		this.mentionedservicecount = mentionedservicecount;
		this.mostdislikedservice = mostdislikedservice;
		this.mostlikedservice = mostlikedservice;
		this.mostnegfrom = mostnegfrom;
		this.mostposifrom = mostposifrom;
		this.regionofusers = regionofusers;
		this.vocinsights = vocinsights;
	}

	public InfluenceOfUsers getInfluenceofusers() {
		return influenceofusers;
	}

	public void setInfluenceofusers(InfluenceOfUsers influenceofusers) {
		this.influenceofusers = influenceofusers;
	}

	public JoinedUsers getJoinedusers() {
		return joinedusers;
	}

	public void setJoinedusers(JoinedUsers joinedusers) {
		this.joinedusers = joinedusers;
	}

	public MentionedServiceCount getMentionedservicecount() {
		return mentionedservicecount;
	}

	public void setMentionedservicecount(MentionedServiceCount mentionedservicecount) {
		this.mentionedservicecount = mentionedservicecount;
	}

	public MostDislikedService[] getMostdislikedservice() {
		return mostdislikedservice;
	}

	public void setMostdislikedservice(MostDislikedService[] mostdislikedservice) {
		this.mostdislikedservice = mostdislikedservice;
	}

	public MostLikedService[] getMostlikedservice() {
		return mostlikedservice;
	}

	public void setMostlikedservice(MostLikedService[] mostlikedservice) {
		this.mostlikedservice = mostlikedservice;
	}

	public RegionDistribution getMostnegfrom() {
		return mostnegfrom;
	}

	public void setMostnegfrom(RegionDistribution mostnegfrom) {
		this.mostnegfrom = mostnegfrom;
	}

	public RegionDistribution getMostposifrom() {
		return mostposifrom;
	}

	public void setMostposifrom(RegionDistribution mostposifrom) {
		this.mostposifrom = mostposifrom;
	}

	public RegionOfUsers getRegionofusers() {
		return regionofusers;
	}

	public void setRegionofusers(RegionOfUsers regionofusers) {
		this.regionofusers = regionofusers;
	}

	public VocInsights getVocinsights() {
		return vocinsights;
	}

	public void setVocinsights(VocInsights vocinsights) {
		this.vocinsights = vocinsights;
	}

}
