package com.ronnie.mapreducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *  多表关联.
 * @author ronnie
 */
public class TableConnections extends Configured implements Tool {
	public static class TableMapper extends Mapper<Text, Text, Text, Text>{
		
		protected void map(Text key, Text value,Context context)
				throws IOException, InterruptedException {
			int i = 0;//定位factoryname和addressname.
			String line = value.toString();
			//不处理第一行标题
			if(line.contains("factoryname")== true || line.contains("addressID") == true){
				return ;
			}
			//判断是否是addressID
			while(line.charAt(i) >= '9' || line.charAt(i) <= '0'){
				i++;
			}
			//写入左表
			if(line.charAt(0) >= '9' || line.charAt(0) <= '0'){
				int j = i - 1;
				while(line.charAt(j) ==' ')
					j--;
				String[] values = {line.substring(0, j),line.substring(i)};
				context.write(new Text(values[1]), new Text("l" + values[0]));
			}else{
				int j = i + 1;
				while(line.charAt(j) == ' '){
					j++;
				}
				String [] values = {line.substring(0,i+1),line.substring(j)};
				context.write(new Text(values[0]), new Text("r" + values[1]));
			}
			
		}
	}
	
	private static int time = 0;
	public static class TableReducer extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			if(time == 0){
				context.write(new Text("factoryName"), new Text("addressName"));
			}
			int factoryNum = 0;
			String[] factory = new String[10];
			
			int addressNum = 0;
			String[] address = new String[10];
			
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext()){
				String tmp = iterator.next().toString();
				char type = tmp.charAt(0);
				
				if(type == 'l'){//处理左表.
					factory[factoryNum] = tmp.substring(2);
					factoryNum++;
				}else{
					address[addressNum] = tmp.substring(2);
				}
			}
			if(factoryNum != 0 && addressNum != 0){
				//做笛卡尔乘积
				for(int m = 0;m<factory.length;m++){
					for(int n = 0;n<address.length;n++){
						context.write(new Text(factory[m]), new Text(address[n]));
					}
				}
				
			}
			
		}
	}
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TableConnections.class);
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		boolean result = job.waitForCompletion(true);
		
		return result ? 0 : -1;
	}
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new TableConnections(), args);
		System.exit(result);
	}

}
