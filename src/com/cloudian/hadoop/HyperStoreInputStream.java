package com.cloudian.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.MDC;

import com.gemini.cloudian.s3.Bucket;
import com.gemini.cloudian.util.Constants;

public class HyperStoreInputStream extends FSInputStream {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreInputStream.class);
	
	private final FileStatus file;
	private long pos;
	private final String hyperStorePath;
	private final HyperStoreBlockLocation[] locations;
	private int currentBlockIndex;
	
	private Bucket bucket;
	private com.gemini.cloudian.fs.Path path;
	
	public HyperStoreInputStream(FileStatus file) throws IOException {
		this.file = file;
		this.pos = 0;
		
		this.hyperStorePath = HyperStoreFileSystem.toHyperStorePath(file);
		this.locations = HyperStoreFileSystem.getHyperStoreBlockLocation(hyperStorePath, 0, file.getLen());
		
		this.currentBlockIndex = 0;
	}

	@Override
	public void seek(long pos) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("seeking to " + pos + " from " + this.pos);
		}
	    if (this.pos == pos) {
	        return;
	    }
	    
	    // TODO: add some exception handlings
	    
	    // move block index
	    for (int i=0; i < this.locations.length; i++) {
	    	HyperStoreBlockLocation location = this.locations[i];
	    	if (location.getOffset() <= pos && 
	    			pos < location.getOffset() + location.getLength()) {
	    		if (LOG.isDebugEnabled()) {
	    			LOG.debug("located the block: " + location + " for " + pos);
	    		}
	    		
	    		// remove previously allocated bytes
	    		this.locations[this.currentBlockIndex].setBytes(null);
	    		
	    		this.currentBlockIndex = i;
	    		break;
	    	}
	    }
	    
	    this.pos = pos;
	}

	@Override
	public long getPos() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getPos is called, returning " + this.pos);
		}
		return this.pos;
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}

	@Override
	public int read() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("read is called");
		}
		
		HyperStoreBlockLocation block = this.locations[this.currentBlockIndex];
		
		if (this.pos + 1 == block.getLength()) {
			return -1;
		}
		
		if (block.getBytes() == null) {
			this.readBytes();
		}
		
		int next = block.getBytes()[Long.valueOf(this.pos++-block.getOffset()).intValue()];
		
		moveIndexIfNecessary();
		
		return next;
	}
	
	private void readBytes() throws IOException {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("will read bytes from " + hyperStorePath);
		}
		
		if (this.bucket == null) {
			bucket = HyperStoreFileSystem.getBucket(hyperStorePath);
			path = HyperStoreFileSystem.toHyperStorePathObject(bucket, hyperStorePath);
		}
		
		HyperStoreBlockLocation block = this.locations[this.currentBlockIndex];
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		MDC.put(Constants.LOG_REQID, "hsfs@" + InetAddress.getLocalHost().getHostAddress());
		HyperStoreFileSystem.cfs.cat(path, block.getOffset(), block.getLength(), baos);
		block.setBytes(baos.toByteArray());
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("read " + block.getBytes().length + " bytes into:" + block.toString());
		}
		
	}
	
	/*
	 * test implementation
	 */
	@Override
	public synchronized int read(byte[] buf, int off, int len) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("read is called with buf length = " + buf.length + " offset = " + off + ", and length = " + len);
		}
		
		HyperStoreBlockLocation block = this.locations[this.currentBlockIndex];
		
		if (block.getBytes() == null) {
			this.readBytes();
		}
		
		int read = Math.min(this.available(), buf.length-off);
		if (read == 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("reached EOF");
			}
			return -1;
		}
		System.arraycopy(block.getBytes(), Long.valueOf(this.pos - block.getOffset()).intValue(), buf, off, read);
		this.pos += read;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("read " + read + " bytes");
		}
		
		moveIndexIfNecessary();
		
		return read;
	}
	
	private void moveIndexIfNecessary() throws IOException {
		if (this.available() == 0 && (this.currentBlockIndex + 1) < this.locations.length) {
			
			this.locations[this.currentBlockIndex].setBytes(null);
			
			this.currentBlockIndex++;
			
			this.readBytes();
			
		}
	}

	@Override
	public int available() throws IOException {
		HyperStoreBlockLocation block = this.locations[this.currentBlockIndex];
		
		if (block.getBytes() == null) {
			this.readBytes();
		}
		
		long left = block.getLength() - (this.pos - block.getOffset());
		if (left > Integer.MAX_VALUE) {
			left = Integer.MAX_VALUE;
		}
		
		return Long.valueOf(left).intValue();
	}

}
