package com.ronnie.mapreduce.prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<NullWritable, Text>{

	private FileSplit split;
	private Configuration conf;
	private Text value;
	private boolean flag;
	/**
	 * 初始化参数
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	/**
	 * 判断是否有key和value对
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!flag){
			//获得分片路径
			Path path = split.getPath();
			byte[] buffer = new byte[(int)split.getLength()];
			FileSystem fileSystem = FileSystem.get(conf);
			FSDataInputStream input = fileSystem.open(path);
			//将一个分片的数据读入到buffer中
			//一次性将split中的数据读取完
			input.read(buffer);
			
			String str = new String(buffer);
			value = new Text(str);//将读取到的值赋给value
			
			flag = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		
	}

}
