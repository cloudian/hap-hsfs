package com.cloudian.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;

public class HyperStoreBlockLocation extends BlockLocation {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreBlockLocation.class);
	
	private final int part;
	private final int subPart;
	private byte[] bytes;
	
	public HyperStoreBlockLocation(String[] names, String[] hosts, long offset, long length) {
		this(names, hosts, offset, length, -1, -1);
	}
	
	public HyperStoreBlockLocation(String[] names, String[] hosts, long offset, 
                       long length, int part, int subPart) {
		super(names, hosts, offset, length);
		
		this.part = part;
		this.subPart = subPart;
	}

	@Override
	public String[] getHosts() throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("getHosts is called");
		}
		return super.getHosts();
	}

	@Override
	public String[] getCachedHosts() {
		if (LOG.isTraceEnabled()) {
			LOG.trace("getCachedHosts is called");
		}
		return super.getCachedHosts();
	}

	@Override
	public String[] getNames() throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("getNames is called");
		}
		return super.getNames();
	}

	@Override
	public String[] getTopologyPaths() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getTopologyPaths is called");
		}
		return super.getTopologyPaths();
	}

	@Override
	public long getOffset() {
		if (LOG.isTraceEnabled()) {
			LOG.trace("getOffset is called");
		}
		return super.getOffset();
	}

	@Override
	public long getLength() {
		if (LOG.isTraceEnabled()) {
			LOG.trace("getLength is called");
		}
		return super.getLength();
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer(super.toString());
		buf.append(",");
		buf.append(this.part);
		buf.append(",");
		buf.append(this.subPart);
		return buf.toString();
	}
	
	public int getPart() {
		return this.part;
	}
	
	public int getSubPart() {
		return this.subPart;
	}
	
	public byte[] getBytes() {
		return this.bytes;
	}
	
	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

}
