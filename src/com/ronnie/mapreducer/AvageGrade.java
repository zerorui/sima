package com.ronnie.mapreducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * 获取学生平均成绩
 * @author ronnie
 *	张三 		12
 *	李四		80
 */
public class AvageGrade extends Configured implements Tool{
	//Mapper 中的几个类型自己决定
	static class AvageMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
		private Text name = new Text();
		private FloatWritable score = new FloatWritable();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			StringTokenizer stringTokenizer = new StringTokenizer(line,"\n");
			
			while(stringTokenizer.hasMoreTokens()){
				String tmp = stringTokenizer.nextToken();
				
				StringTokenizer tmpToken = new StringTokenizer(tmp);
				String nameStr = tmpToken.nextToken();
				String scoreStr = tmpToken.nextToken();
				
				name.set(nameStr);
				score.set(Float.valueOf(scoreStr));
				
				context.write(name, score);
			}
			
		}
	}
	/**
	 * 根据得到进行整理，计算平均分
	 * @author ronnie
	 *
	 */
	static class AvageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		private FloatWritable outScore = new FloatWritable();
		@Override
		protected void reduce(Text name, Iterable<FloatWritable> scores,Context context)
				throws IOException, InterruptedException {
			int count = 0;
			float totalScore = 0;
			for (FloatWritable score : scores) {
				totalScore+=score.get();
				count++;
			}
			outScore.set(totalScore/count);
			context.write(name, outScore);
			
		}
	}
	/**
	 * 配置运行环境
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(AvageGrade.class);
		
		job.setMapperClass(AvageMapper.class);
		job.setReducerClass(AvageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		boolean result = job.waitForCompletion(true);
		
		return result ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new AvageGrade(), args);
		System.exit(result);
	}
	
}
