package com.cloudian.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class HyperStoreInputSplit extends FileSplit {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreInputSplit.class);
	
	private final HyperStoreLocationInfo[] locationInfo;
	
	public HyperStoreInputSplit(Path file, long start, long length, String[] hosts) {
		super(file, start, length, hosts);
		
		// resolve
		String localPath = HyperStoreInputSplit.resolveLocalPath(file);
		this.locationInfo = new HyperStoreLocationInfo[hosts.length];
		for (int i=0; i<locationInfo.length; i++) {
			this.locationInfo[i] = new HyperStoreLocationInfo(hosts[i], false, localPath);
		}
	}
	
	private static String resolveLocalPath(Path file) {
		LOG.debug("resolveLocalPath was called");
		return null;
	}

	@Override
	public SplitLocationInfo[] getLocationInfo() throws IOException {
		LOG.debug("getLocationInfo was called");
		return this.locationInfo;
	}

}
