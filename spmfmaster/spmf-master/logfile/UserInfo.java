package logfile;

import java.sql.Clob;
import java.sql.Timestamp;

public class UserInfo {

	private Clob url;
	private Timestamp ts;

	public UserInfo(Clob url, Timestamp ts) {
		this.url = url;
		this.ts = ts;
	}

	public Clob getUrl() {
		return this.url;
	}

	public Timestamp getTimeStamp() {
		return this.ts;
	}

}
