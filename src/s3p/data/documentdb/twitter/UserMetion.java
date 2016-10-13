package s3p.data.documentdb.twitter;

import org.msgpack.annotation.Message;

@Message
public class UserMetion {
	private String screen_name;
	private String name;
	private Long id;
	private String id_str;
	private int[] indices;

	public UserMetion() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getId_str() {
		return id_str;
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}

	public int[] getIndices() {
		return indices;
	}

	public void setIndices(int[] indices) {
		this.indices = indices;
	}

}
