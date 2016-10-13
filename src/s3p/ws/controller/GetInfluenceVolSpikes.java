package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.InfluenceVolSpikesService;

public class GetInfluenceVolSpikes extends Controller {
	
	private InfluenceVolSpikesService service = new InfluenceVolSpikesService();

	public void index() {
		String platform=getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), days);
		renderJson(json);
	}
}
