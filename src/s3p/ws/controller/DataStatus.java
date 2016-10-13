package s3p.ws.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.jfinal.core.Controller;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableQuery;

import s3p.data.storage.table.JobEntity;
import s3p.data.utils.TableUtils;

public class DataStatus extends Controller {

	public void index() {
		CloudTable table = TableUtils.getTable("Job");
		TableQuery<JobEntity> query = TableQuery.from(JobEntity.class);
		List<JobEntity> list = new ArrayList<>();
		Iterator<JobEntity> iterator = table.execute(query).iterator();
		while(iterator.hasNext()) {
			list.add(iterator.next());
		}
		setAttr("statusList", list);
		render("index.ftl");;
	}
}
