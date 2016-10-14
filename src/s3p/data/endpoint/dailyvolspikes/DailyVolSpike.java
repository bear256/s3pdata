package s3p.data.endpoint.dailyvolspikes;

import s3p.data.endpoint.common.Sentiment;

public class DailyVolSpike {

	private long dailytimeslot;
	private int dailyundefinfluencespike;
	private int dailyundefinfluencevol;
	private int dailyundefspike;
	private int dailyundefvol;
	private int dailyneginfluencespike;
	private int dailyneginfluencevol;
	private int dailynegspike;
	private int dailynegvol;
	private int dailyneuinfluencespike;
	private int dailyneuinfluencevol;
	private int dailyneuspike;
	private int dailyneuvol;
	private int dailyposiinfluencespike;
	private int dailyposiinfluencevol;
	private int dailyposispike;
	private int dailyposivol;
	private int dailyinfluencespikevol;
	private int dailyspikevol;
	private int dailytotalinfluencevol;
	private int dailytotalvol;

	public DailyVolSpike() {
		super();
	}

	public void incVocInfluence(int sentiment, int influenceCount) {
		switch (sentiment) {
		case Sentiment.UNDEFINED:// Negative
			dailyundefvol++;
			dailyundefinfluencevol += influenceCount;
			break;
		case Sentiment.NEGATIVE:// Negative
			dailynegvol++;
			dailyneginfluencevol += influenceCount;
			break;
		case Sentiment.NEUTRAL:// Negative
			dailyneuvol++;
			dailyneuinfluencevol += influenceCount;
			break;
		case Sentiment.POSITIVE:// Positive
			dailyposivol++;
			dailyposiinfluencevol += influenceCount;
			break;
		}
		dailytotalinfluencevol += influenceCount;
		dailytotalvol++;
	}

	public void merge(DailyVolSpike daily) {
		this.dailyundefvol += daily.getDailyundefvol();
		this.dailyundefinfluencevol += daily.getDailyundefinfluencevol();
		this.dailynegvol += daily.getDailynegvol();
		this.dailyneginfluencevol += daily.getDailyneginfluencevol();
		this.dailyneuvol += daily.getDailyneuvol();
		this.dailyneuinfluencevol += daily.getDailyneuinfluencevol();
		this.dailyposivol += daily.getDailyposivol();
		this.dailyposiinfluencevol += daily.getDailyposiinfluencevol();
		this.dailytotalvol += daily.getDailytotalvol();
		this.dailytotalinfluencevol += daily.getDailytotalinfluencevol();
	}

	public int getDailyundefinfluencespike() {
		return dailyundefinfluencespike;
	}

	public void setDailyundefinfluencespike(int dailyundefinfluencespike) {
		this.dailyundefinfluencespike = dailyundefinfluencespike;
	}

	public int getDailyundefinfluencevol() {
		return dailyundefinfluencevol;
	}

	public void setDailyundefinfluencevol(int dailyundefinfluencevol) {
		this.dailyundefinfluencevol = dailyundefinfluencevol;
	}

	public int getDailyundefspike() {
		return dailyundefspike;
	}

	public void setDailyundefspike(int dailyundefspike) {
		this.dailyundefspike = dailyundefspike;
	}

	public int getDailyundefvol() {
		return dailyundefvol;
	}

	public void setDailyundefvol(int dailyundefvol) {
		this.dailyundefvol = dailyundefvol;
	}

	public int getDailyneuinfluencespike() {
		return dailyneuinfluencespike;
	}

	public void setDailyneuinfluencespike(int dailyneuinfluencespike) {
		this.dailyneuinfluencespike = dailyneuinfluencespike;
	}

	public int getDailyneuinfluencevol() {
		return dailyneuinfluencevol;
	}

	public void setDailyneuinfluencevol(int dailyneuinfluencevol) {
		this.dailyneuinfluencevol = dailyneuinfluencevol;
	}

	public int getDailyneuspike() {
		return dailyneuspike;
	}

	public void setDailyneuspike(int dailyneuspike) {
		this.dailyneuspike = dailyneuspike;
	}

	public int getDailyneuvol() {
		return dailyneuvol;
	}

	public void setDailyneuvol(int dailyneuvol) {
		this.dailyneuvol = dailyneuvol;
	}

	public int getDailyinfluencespikevol() {
		return dailyinfluencespikevol;
	}

	public void setDailyinfluencespikevol(int dailyinfluencespikevol) {
		this.dailyinfluencespikevol = dailyinfluencespikevol;
	}

	public int getDailyneginfluencespike() {
		return dailyneginfluencespike;
	}

	public void setDailyneginfluencespike(int dailyneginfluencespike) {
		this.dailyneginfluencespike = dailyneginfluencespike;
	}

	public int getDailyneginfluencevol() {
		return dailyneginfluencevol;
	}

	public void setDailyneginfluencevol(int dailyneginfluencevol) {
		this.dailyneginfluencevol = dailyneginfluencevol;
	}

	public int getDailynegspike() {
		return dailynegspike;
	}

	public void setDailynegspike(int dailynegspike) {
		this.dailynegspike = dailynegspike;
	}

	public int getDailynegvol() {
		return dailynegvol;
	}

	public void setDailynegvol(int dailynegvol) {
		this.dailynegvol = dailynegvol;
	}

	public int getDailyposiinfluencespike() {
		return dailyposiinfluencespike;
	}

	public void setDailyposiinfluencespike(int dailyposiinfluencespike) {
		this.dailyposiinfluencespike = dailyposiinfluencespike;
	}

	public int getDailyposiinfluencevol() {
		return dailyposiinfluencevol;
	}

	public void setDailyposiinfluencevol(int dailyposiinfluencevol) {
		this.dailyposiinfluencevol = dailyposiinfluencevol;
	}

	public int getDailyposispike() {
		return dailyposispike;
	}

	public void setDailyposispike(int dailyposispike) {
		this.dailyposispike = dailyposispike;
	}

	public int getDailyposivol() {
		return dailyposivol;
	}

	public void setDailyposivol(int dailyposivol) {
		this.dailyposivol = dailyposivol;
	}

	public int getDailyspikevol() {
		return dailyspikevol;
	}

	public void setDailyspikevol(int dailyspikevol) {
		this.dailyspikevol = dailyspikevol;
	}

	public long getDailytimeslot() {
		return dailytimeslot;
	}

	public void setDailytimeslot(long timeslot) {
		this.dailytimeslot = timeslot;
	}

	public int getDailytotalinfluencevol() {
		return dailytotalinfluencevol;
	}

	public void setDailytotalinfluencevol(int dailytotalinfluencevol) {
		this.dailytotalinfluencevol = dailytotalinfluencevol;
	}

	public int getDailytotalvol() {
		return dailytotalvol;
	}

	public void setDailytotalvol(int dailytotalvol) {
		this.dailytotalvol = dailytotalvol;
	}

}
