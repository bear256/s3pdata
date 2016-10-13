package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.MessageVolSpikesService;

public class GetMessageVolSpikes extends Controller {

	private MessageVolSpikesService service = new MessageVolSpikesService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String date = getPara("date", "");
		String json = "";
		if ("".equals(date)) {
			json = service.get(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), days);
		} else {
			json = service.getByDate(platform.toLowerCase(), topic.toUpperCase(), pnScope.toUpperCase(), date);
		}
		renderJson(json);
	}
}
