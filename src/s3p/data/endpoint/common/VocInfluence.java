package s3p.data.endpoint.common;

import org.msgpack.annotation.Message;

@Message
public class VocInfluence {

	private Integer detectednegspikesvol = 0;
	private Integer detectedposispikesvol = 0;
	private Integer detectedspikesvol = 0;
	private Integer undefinedinfluencedvol = 0;
	private Integer undefinedtotalvol = 0;
	private Integer negativeinfluencedvol = 0;
	private Integer negativetotalvol = 0;
	private Integer neutralinfluencedvol = 0;
	private Integer neutraltotalvol = 0;
	private Integer positiveinfluencedvol = 0;
	private Integer positivetotalvol = 0;
	private Integer uniqueuserregion = 0;
	private Integer uniqueusers = 0;
	private Integer vocinfluencedvol = 0;
	private Integer voctotalvol = 0;
	private Integer voclasttimetotalvolume = 0;
	private double vocvolgrowthratio = 0;

	public VocInfluence() {
		super();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		switch (sentiment) {
		case Sentiment.UNDEFINED:
			undefinedtotalvol++;
			undefinedinfluencedvol += influenceCount;
			break;
		case Sentiment.NEGATIVE:// Negative
			negativetotalvol++;
			negativeinfluencedvol += influenceCount;
			break;
		case Sentiment.NEUTRAL:
			neutraltotalvol++;
			neutralinfluencedvol += influenceCount;
			break;
		case Sentiment.POSITIVE:// Positive
			positivetotalvol++;
			positiveinfluencedvol += influenceCount;
			break;
		}
		vocinfluencedvol += influenceCount;
		voctotalvol++;
	}

	public void merge(VocInfluence influence) {
		detectednegspikesvol += influence.getDetectednegspikesvol();
		detectedposispikesvol += influence.getDetectedposispikesvol();
		detectedspikesvol += influence.getDetectedspikesvol();
		undefinedinfluencedvol += influence.getUndefinedinfluencedvol();
		undefinedtotalvol += influence.getUndefinedtotalvol();
		negativeinfluencedvol += influence.getNegativeinfluencedvol();
		negativetotalvol += influence.getNegativetotalvol();
		neutralinfluencedvol += influence.getNeutralinfluencedvol();
		neutraltotalvol += influence.getNeutraltotalvol();
		positiveinfluencedvol += influence.getPositiveinfluencedvol();
		positivetotalvol += influence.getPositivetotalvol();
		uniqueuserregion += influence.getUniqueuserregion();
		uniqueusers += influence.getUniqueusers();
		vocinfluencedvol += influence.getVocinfluencedvol();
		voctotalvol += influence.getVoctotalvol();
	}

	public Integer getDetectednegspikesvol() {
		return detectednegspikesvol;
	}

	public void setDetectednegspikesvol(Integer detectednegspikesvol) {
		this.detectednegspikesvol = detectednegspikesvol;
	}

	public Integer getDetectedposispikesvol() {
		return detectedposispikesvol;
	}

	public void setDetectedposispikesvol(Integer detectedposispikesvol) {
		this.detectedposispikesvol = detectedposispikesvol;
	}

	public Integer getDetectedspikesvol() {
		return detectedspikesvol;
	}

	public void setDetectedspikesvol(Integer detectedspikesvol) {
		this.detectedspikesvol = detectedspikesvol;
	}

	public Integer getUndefinedinfluencedvol() {
		return undefinedinfluencedvol;
	}

	public void setUndefinedinfluencedvol(Integer undefinedinfluencedvol) {
		this.undefinedinfluencedvol = undefinedinfluencedvol;
	}

	public Integer getUndefinedtotalvol() {
		return undefinedtotalvol;
	}

	public void setUndefinedtotalvol(Integer undefinedtotalvol) {
		this.undefinedtotalvol = undefinedtotalvol;
	}

	public Integer getNegativeinfluencedvol() {
		return negativeinfluencedvol;
	}

	public void setNegativeinfluencedvol(Integer negativeinfluencedvol) {
		this.negativeinfluencedvol = negativeinfluencedvol;
	}

	public Integer getNegativetotalvol() {
		return negativetotalvol;
	}

	public void setNegativetotalvol(Integer negativetotalvol) {
		this.negativetotalvol = negativetotalvol;
	}

	public Integer getNeutralinfluencedvol() {
		return neutralinfluencedvol;
	}

	public void setNeutralinfluencedvol(Integer neutralinfluencedvol) {
		this.neutralinfluencedvol = neutralinfluencedvol;
	}

	public Integer getNeutraltotalvol() {
		return neutraltotalvol;
	}

	public void setNeutraltotalvol(Integer neutraltotalvol) {
		this.neutraltotalvol = neutraltotalvol;
	}

	public Integer getPositiveinfluencedvol() {
		return positiveinfluencedvol;
	}

	public void setPositiveinfluencedvol(Integer positiveinfluencedvol) {
		this.positiveinfluencedvol = positiveinfluencedvol;
	}

	public Integer getPositivetotalvol() {
		return positivetotalvol;
	}

	public void setPositivetotalvol(Integer positivetotalvol) {
		this.positivetotalvol = positivetotalvol;
	}

	public Integer getUniqueuserregion() {
		return uniqueuserregion;
	}

	public void setUniqueuserregion(Integer uniqueuserregion) {
		this.uniqueuserregion = uniqueuserregion;
	}

	public Integer getUniqueusers() {
		return uniqueusers;
	}

	public void setUniqueusers(Integer uniqueusers) {
		this.uniqueusers = uniqueusers;
	}

	public Integer getVocinfluencedvol() {
		return vocinfluencedvol;
	}

	public void setVocinfluencedvol(Integer vocinfluencedvol) {
		this.vocinfluencedvol = vocinfluencedvol;
	}

	public Integer getVoctotalvol() {
		return voctotalvol;
	}

	public void setVoctotalvol(Integer voctotalvol) {
		this.voctotalvol = voctotalvol;
	}

	public Integer getVoclasttimetotalvolume() {
		return voclasttimetotalvolume;
	}

	public void setVoclasttimetotalvolume(Integer voclasttimetotalvolume) {
		this.voclasttimetotalvolume = voclasttimetotalvolume;
		this.vocvolgrowthratio = 1.0 * (voctotalvol - voclasttimetotalvolume)
				/ (voclasttimetotalvolume != 0 ? voclasttimetotalvolume : 1);
	}

	public double getVocvolgrowthratio() {
		return vocvolgrowthratio;
	}

	public void setVocvolgrowthratio(double vocvolgrowthratio) {
		this.vocvolgrowthratio = vocvolgrowthratio;
	}

}
