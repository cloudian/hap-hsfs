package com.cloudian.hadoop.pig;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.pig.builtin.PigStorage;

import com.cloudian.hadoop.HyperStoreInputFormat;

public class HyperStoreStorage extends PigStorage {

	@Override
	public InputFormat getInputFormat() {
		return new HyperStoreInputFormat();
	}

}
