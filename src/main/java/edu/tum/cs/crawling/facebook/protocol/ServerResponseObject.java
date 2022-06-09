package edu.tum.cs.crawling.facebook.protocol;

import java.io.Serializable;

public class ServerResponseObject implements Serializable {

	public static enum Status {
		UNKOWN, OK, ERROR, RETRY, TERMINATE
	}

	private static final long serialVersionUID = -1104740599611103266L;

	private String id = null;
	private Status status = Status.UNKOWN;
	private ServerStatistics statistics = null;

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ServerStatistics getStatistics() {
		return statistics;
	}

	public void setStatistics(ServerStatistics statistics) {
		this.statistics = statistics;
	}

}
