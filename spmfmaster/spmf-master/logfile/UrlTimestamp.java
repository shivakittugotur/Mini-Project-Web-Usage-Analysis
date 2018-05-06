package logfile;

public class UrlTimestamp {
	private long secs;
	String url;

	public UrlTimestamp(String url, long secs) {
		this.secs = secs;
		this.url = url;
	}

	public String getUrl() {
		return this.url;
	}

	public long getSecs() {
		return this.secs;
	}
}
