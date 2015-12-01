package com.ronnie.mapreducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 对多个文件输入的数据进行排序.
 * @author ronnie
 */
public class NumSort extends Configured implements Tool{
	public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		private static IntWritable data = new IntWritable();
		
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			int num = Integer.parseInt(line);
			data.set(num);
			
			context.write(data, new IntWritable(1));
		}
	}
	
	/**
	 * 保证前一个分片的数一定比后一个分片的数大.
	 */
	public static class Partition extends Partitioner<IntWritable, IntWritable>{

		public int getPartition(IntWritable key, IntWritable value,int numPartitions) {
			int maxData = 10000;
			int bound = maxData / numPartitions + 1;
			
			int num = key.get();
			for(int i = 0; i < num ; i++){
				if(num < bound * (i+1) && num > bound * i){
					return i;
				}
			}
			
			return -1;
		}
		
	}
	
	/**
	 * 对map端输出的内容进行排序.
	 */
	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static IntWritable lineNum = new IntWritable(1);
		protected void reduce(IntWritable num,Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			for(@SuppressWarnings("unused") IntWritable val : values){
				context.write(lineNum, num);
				lineNum = new IntWritable(lineNum.get() + 1);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(NumSort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		
		return result ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new NumSort(), args);
		System.exit(result);
	}
	
}
