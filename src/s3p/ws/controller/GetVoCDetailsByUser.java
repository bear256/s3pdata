package s3p.ws.controller;

import com.jfinal.core.Controller;

import s3p.ws.service.VoCDetailsService;

public class GetVoCDetailsByUser extends Controller {

	private VoCDetailsService service = new VoCDetailsService();

	public void index() {
		String platform = getPara("platform");
		String topic = getPara("topic");
		String userId = getPara("userid");
		String pnScope = getPara("PNScope");
		int days = getParaToInt("days", 7);
		String json = service.getByUser(platform.toLowerCase(), topic.toUpperCase(), userId, pnScope.toUpperCase(),
				days);
		renderJson(json);
	}
}
