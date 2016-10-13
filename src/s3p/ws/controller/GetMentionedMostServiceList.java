package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.MentionedMostServiceListService;

public class GetMentionedMostServiceList extends Controller {
	
	private MentionedMostServiceListService service = new MentionedMostServiceListService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase());
		renderJson(json);
	}
}
