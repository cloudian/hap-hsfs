package com.cloudian.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.SplitLocationInfo;

public class HyperStoreLocationInfo extends SplitLocationInfo {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreLocationInfo.class);
	
	private final String localPath;

	public HyperStoreLocationInfo(String location, boolean inMemory, String localPath) {
		super(location, inMemory);
		this.localPath = localPath;
	}
	
	public String getLocalPath() {
		LOG.debug("getLocalPath was called");
		return this.localPath;
	}

}
