package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.UserVolSpikesService;

public class GetUserVolSpikes extends Controller {
	
	private UserVolSpikesService service = new UserVolSpikesService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), days);
		renderJson(json);
	}
}
