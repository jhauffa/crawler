package edu.tum.cs.crawling.livejournal.framework;

public interface Consumer {

	public void handleResource(Resource res, Object param);

}
