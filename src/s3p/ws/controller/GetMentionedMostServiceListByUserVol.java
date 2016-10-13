package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.MentionedMostServiceListByUserVolService;

public class GetMentionedMostServiceListByUserVol extends Controller {
	
	private MentionedMostServiceListByUserVolService service = new MentionedMostServiceListByUserVolService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase());
		renderJson(json);
	}
}
