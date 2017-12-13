package com.neu.pdpmr.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author Tom White
 * This class is used to force that each file goes exactly to one Mapper
 */
public class NonSplitableTextInputFormat extends TextInputFormat {

    @Override
    protected boolean isSplitable(JobContext context, Path path) {
        return false;
    }
}