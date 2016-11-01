package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.MentionedMostServiceListService;

public class GetMentionedMostServiceList extends Controller {
	
	private MentionedMostServiceListService service = new MentionedMostServiceListService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		long date = getParaToLong("date", 0L);
		String datetype = getPara("datetype", "w");
		String serviceName = getPara("servicename", "");
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), date, datetype, serviceName);
		renderJson(json);
	}
}
