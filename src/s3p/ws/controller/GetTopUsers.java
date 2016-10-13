package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.TopUsersService;

public class GetTopUsers extends Controller {

	private TopUsersService service = new TopUsersService();

	public void index() {
		String platform = getPara("platform");
		int topNum = getParaToInt("topNum", 5);
		String topic = getPara("topic");
		String pnScope = getPara("PNScope");
		String json = service.get(platform.toLowerCase(), topNum, topic.toUpperCase(), pnScope.toUpperCase());
		renderJson(json);
	}
}
