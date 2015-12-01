package com.ronnie.mapreducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 单词的统计
 * @author ronnie
 */
public class WordCount extends Configured implements Tool{
	/**
	 * Map 类 :逐行进行读取,所以Value 是一行的值.
	 */
	static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text word = new Text();
		private IntWritable count = new IntWritable(1);
		/*
		 * 覆盖父类的map方法.
		 */
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//获取一行的内容
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			
			while(stringTokenizer.hasMoreTokens()){
				String workValue = stringTokenizer.nextToken();
				word.set(workValue);
				//每得到一个单词，就写入到reduce中.
				context.write(word, count);
			}
			
		}
	}
	/** 
	 * Reduce 处理类
	 */
	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		/*
		 * 从map中传递的数据
		 */
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int count = 0;//出现的次数
			
			for (IntWritable value : values) {
				count+=value.get() ;
			}
			
			result.set(count);
			//写入结果
			context.write(key, result);
		}
	}
	/*
	 * Driver 部分
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new WordCount(), args);
		//结束程序
		System.exit(result);
	}
	@Override
	public int run(String[] args) throws Exception {
		//获取配置信息
		Configuration configuration = getConf();
		//获取job
		Job job = Job.getInstance(configuration, "wordcount");
		//设置Job运行的类
		job.setJarByClass(WordCount.class);
		
		
		//设置输出结果的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置mapper和reducer类
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		//设置输出和输入目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//提交job
		boolean result = job.waitForCompletion(true);
		return result ? 0 : -1;
	}
}
