package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.MentionedMostServiceTrendService;

public class GetMentionedMostServiceTrend extends Controller {

	private MentionedMostServiceTrendService service = new MentionedMostServiceTrendService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		String serviceName = getPara("servicename", "");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), serviceName);
		renderJson(json);
	}
}
