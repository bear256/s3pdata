package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.DailyVolSpikesService;

public class GetDailyVolSpikes extends Controller {
	
	private DailyVolSpikesService service = new DailyVolSpikesService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		int days = getParaToInt("days", 7);
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), days);
		renderJson(json);
	}
}
