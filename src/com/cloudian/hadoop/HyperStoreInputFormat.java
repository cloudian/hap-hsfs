package com.cloudian.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HyperStoreInputFormat extends TextInputFormat {
	
	private static final Log LOG = LogFactory.getLog(HyperStoreInputFormat.class);
	
	/*
	 * FileInputFormat#getSplits does  this:
	 * 
	 *   FileSystem fs = path.getFileSystem(job.getConfiguration());
	 *   blkLocations = fs.getFileBlockLocations(file, start, length);
	 *   splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
     *                 blkLocations[blkIndex].getHosts(),
     *                 blkLocations[blkIndex].getCachedHosts()));
     * 
     * So, the easiest is to override FileSystem#getFileBlockLocations,
     * and leave this as is.
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		LOG.debug("getSplits was called");
		return super.getSplits(job);
	}

	/*
	 * The default implementation returns LineRecordReader,
	 * which opens a file via FileSystem#open.
	 * So, our own RecordReader that directly reads from local disk is required.
	 * Our own information needs to be provided via our own InputSplit.
	 */
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) {
		LOG.debug("createRecordReader was called");
		return super.createRecordReader(split, context);
	}
	
	/*
	 * This is called in getSplits,
	 * and returns our own InputSplit that contains extra info like local file path
	 */
	@Override
	protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
		LOG.debug("makeSplit was called");
		return new HyperStoreInputSplit(file, start, length, hosts);
	}

}
