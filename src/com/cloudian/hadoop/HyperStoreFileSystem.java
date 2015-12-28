package com.cloudian.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.HybridToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;

import com.cloudian.config.S3Configuring;
import com.cloudian.ss.cassandra.cl.ProtectionPolicy;
import com.gemini.cloudian.fs.CFSUtil;
import com.gemini.cloudian.fs.CassandraFileSystem;
import com.gemini.cloudian.hybrid.client.HybridCassandra;
import com.gemini.cloudian.s3.Bucket;
import com.gemini.cloudian.s3.PartInfo;
import com.gemini.cloudian.s3.PartRange;
import com.gemini.cloudian.s3.UploadedParts;
import com.gemini.cloudian.ss.redis.RedisBucketStore;
import com.gemini.cloudian.util.ProtectionPolicyUtil;

/**
 * HyperStoreFileSystem is a FileSystem that accesses HyperStore directly to read a file.
 * Metadata operations are done through a standard S3 APIs with S3AFileSystem.
 * So, if not sufficient privilege is granted, read operation will fail at metadata retrieval.
 * 
 * No write operations is available in this FileSystem to make clear the purpose of this filesystem.
 * Instead, a user should use S3AFileSystem.
 * 
 * @author tsato
 *
 */
public class HyperStoreFileSystem extends S3AFileSystem {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreFileSystem.class);
	static final CassandraFileSystem cfs = CassandraFileSystem.getInstance();
	
	public HyperStoreFileSystem() {
		super();
	}
	
	@Override
	public void initialize(URI name, Configuration conf) throws IOException {
	    super.initialize(name, conf);
	    // do our own initialization here
		if (LOG.isDebugEnabled()) {
			LOG.debug("initialize is called");
		}
	}

	@Override
	public String getScheme() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getSchema is called");
		}
		return "hsfs";
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getFileBlockLocations for " + file.toString() + ":" + start + "-" + len);
		}
		if (file == null) {
			return null;
		}

		if (start < 0 || len < 0) {
			throw new IllegalArgumentException("Invalid start or len parameter");
		}

		if (file.getLen() <= start) {
			return new BlockLocation[0];
		}
		
		String hyperStorePath = toHyperStorePath(file);
		return HyperStoreFileSystem.getHyperStoreBlockLocation(hyperStorePath, start, len);
	}
	
	static String toHyperStorePath(FileStatus file) {
		URI uri = file.getPath().toUri();
		String hyperStorePath = uri.getAuthority() + uri.getPath();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(file.getPath().toString() + " => " + hyperStorePath);
		}
		
		return hyperStorePath;
	}
	
	static com.gemini.cloudian.fs.Path toHyperStorePathObject(Bucket bucket, String hyperStorePath) throws IOException {
		
		// create target Path object
		com.gemini.cloudian.fs.Path target = new com.gemini.cloudian.fs.Path(bucket.getTenantId(), 
													  bucket.getUserId(),
													  bucket.getCanonicalUserId(),
													  hyperStorePath,
													  null,
													  true,
													  bucket.hasOwnRow());
		
		// read metadata first
		cfs.ls(target, true, S3Configuring.getInstance().readFromWideRow());
		
		return target;
	}
	
	static Bucket getBucket(String hyperStorePath) {
		String bucketName = com.gemini.cloudian.fs.Path.getBucketNameFromPath(hyperStorePath);
		Bucket bucket = RedisBucketStore.getBucketInfo(bucketName);
		return bucket;
	}
	
	static HyperStoreBlockLocation[] getHyperStoreBlockLocation(String hyperStorePath, long start, long len) throws IOException {
		
		Bucket bucket = getBucket(hyperStorePath);
		com.gemini.cloudian.fs.Path target = toHyperStorePathObject(bucket, hyperStorePath);
		boolean isChunked = target.isChunked();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("getting HyperStore hosts for: " + target.toString() + ", start=" + start + ", len=" + len + ", isChunked=" + isChunked);
		}
		
		if (!target.isHSFS()) {
			String[] names = {"localhost:19050"};
			String[] hosts = {"localhost"};
			HyperStoreBlockLocation[] locations = {new HyperStoreBlockLocation(names, hosts, 0, target.getSize())};
			return locations;
		}
		
		if (target.isChunked()) {
			return getChunkedHyperStoreBlockLocations(bucket, target, start, len);
		}
		
		List<String> endpoints = getEndpoints(bucket, target, -1, -1);
		HyperStoreBlockLocation location = getBlockLocation(endpoints, start, len, -1, -1);
		if (LOG.isDebugEnabled()) {
			LOG.debug("got location: " + location);
		}
		
		return new HyperStoreBlockLocation[]{location};
	}
	
	private static HyperStoreBlockLocation[] getChunkedHyperStoreBlockLocations(Bucket bucket, com.gemini.cloudian.fs.Path target, long start, long len) throws IOException {
        
		if (LOG.isDebugEnabled()) {
			LOG.debug("getting chunked HyperStoreBlockInfo");
		}
		
		List<HyperStoreBlockLocation> locations = new ArrayList<HyperStoreBlockLocation>();
		
		UploadedParts uparts =  new UploadedParts(target.getPart(),
                target.getPartSize(),
                target.getPartInfo());
        PartRange prange = uparts.getRange(start, len);
        
        int startPart = prange.getStartIndex();
        int endPart = prange.getEndIndex();
        long offset = prange.getOffset();
        
        if (LOG.isDebugEnabled()) {
        	LOG.debug("startPart = " + startPart + ", endPart = " + endPart + ", offset = " + offset);
        }
        
        for (int i = startPart; i <= endPart; i++) {
        	
        	PartInfo pi = uparts.getPart(i);
            int subParts = pi.getNumSubParts();
            
        	if (LOG.isDebugEnabled()) {
        		LOG.debug(subParts + " subParts in " + i);
        	}
        	
            if (subParts > 0) {
            	
                int j = (i == startPart && offset > 0) ? prange.getStartSubPart() : 1;
                
                for (; j <= subParts; j++) {
                	// a chunked sub part is a block
                    List<String> endpoints = getEndpoints(bucket, target, i, j);
                    HyperStoreBlockLocation location = getBlockLocation(endpoints, offset, pi.getPartSize(j), i, j);
                    locations.add(location);
                    
                    if (LOG.isDebugEnabled()) {
                    	LOG.debug("added: " + location + " at " + j);
                    }
                    
                    offset += pi.getPartSize(j);
                }
            } else {
            	// a chunked part is a block
                List<String> endpoints = getEndpoints(bucket, target, i, -1);
                HyperStoreBlockLocation location = getBlockLocation(endpoints, offset, pi.getPartSize(), i, -1);
                locations.add(location);
                
                if (LOG.isDebugEnabled()) {
                	LOG.debug("added: " + location);
                }
                
                offset += pi.getPartSize();
            }
        	
        }
		
		return locations.toArray(new HyperStoreBlockLocation[locations.size()]);
	}
	
	private static List<String> getEndpoints(Bucket bucket, com.gemini.cloudian.fs.Path target, int partNumber, int subPartNumber) throws IOException {
		String endpointPath = null;
        final ProtectionPolicy pp = ProtectionPolicyUtil.getProtectionPolicy(bucket);
        final String ks = pp.getKeyspaceName();
		
		if (partNumber == -1) {
			endpointPath = CFSUtil.composePath(target.getPath(), target.getVersion());
		} else if (subPartNumber == -1) {
			endpointPath = CFSUtil.composePartPath(target.getPath(), partNumber, target);
		} else {
			endpointPath = CFSUtil.composePartPath(target.getPath(), partNumber, subPartNumber, target);
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("endpointPath: " + endpointPath);
		}
		
        HybridToken tr = HybridCassandra.getInstance().getTokenAndEndPoints(
                ks, endpointPath, pp.getReplicationScheme());
        List<String> endpoints = tr.getEndpoints();
        
        return endpoints;
	}
	
	private static HyperStoreBlockLocation getBlockLocation(List<String> endpoints, long offset, long length, int part, int subPart) {
        String[] hosts = endpoints.toArray(new String[endpoints.size()]);
        String[] names = new String[hosts.length];
        for (int n=0; n<names.length; n++) {
        	names[n] = hosts[n] + ":19050";
        }
        HyperStoreBlockLocation location = new HyperStoreBlockLocation(names, hosts, offset, length, part, subPart);
        return location;
	}
	
	/**
	 * @See FileInputFormat#computeSplitSize
	 */
	@Override
	public long getDefaultBlockSize() {
		//return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
		return S3Configuring.getInstance().getMaxObjectChunkSize();
	}

	/*
	 * return our own FSDataInputStream, in which actual read operations is executed
	 * (non-Javadoc)
	 * @see org.apache.hadoop.fs.s3a.S3AFileSystem#open(org.apache.hadoop.fs.Path, int)
	 */
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		
	    if (LOG.isDebugEnabled()) {
	        LOG.debug("Opening " + f.toString() + " for reading.");
	    }
	    final FileStatus fileStatus = getFileStatus(f);
	    if (fileStatus.isDirectory()) {
	      throw new FileNotFoundException("Can't open " + f + " because it is a directory");
	    }
	    
		return new FSDataInputStream(new HyperStoreInputStream(fileStatus));
	}
	
	/* S3 API */
	@Override
	public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("listStatus is called");
		}
		return super.listStatus(f);
	}
	
	/* Not Implemented */
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
	    throw new UnsupportedOperationException("Not implemented by the " + 
	            getClass().getSimpleName() + " FileSystem implementation");
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, 
			int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		throw new UnsupportedOperationException("Not implemented by the " + 
				getClass().getSimpleName() + " FileSystem implementation");
	}
	
	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		throw new UnsupportedOperationException("Not implemented by the " + 
				getClass().getSimpleName() + " FileSystem implementation");
	}
	
	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		throw new UnsupportedOperationException("Not implemented by the " + 
				getClass().getSimpleName() + " FileSystem implementation");
	}
	
	@Override
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
		throw new UnsupportedOperationException("Not implemented by the " + 
				getClass().getSimpleName() + " FileSystem implementation");
	}

}
