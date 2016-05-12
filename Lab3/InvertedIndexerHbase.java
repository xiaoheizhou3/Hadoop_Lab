package cn.nju.edu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.hbase.util.Bytes;

public class InvertedIndexerHbase {
	  private static Configuration configuration = HBaseConfiguration.create();	    
	  private static ArrayList<String> wordArrayList = new ArrayList<>();
	  
	  public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
		  //    private Set<String> stopwords;
		  //    private Path[] localFiles;   
		  public int getCharacterPosition(String string){
			  Matcher slashMatcher = Pattern.compile("(\\.txt)|(\\.TXT)").matcher(string);
			  if(slashMatcher.find()){
				  return slashMatcher.start();
			  }
			  return 0;
		  }
    
		  public void map( Object key, Text value,Context context)
				  throws IOException,InterruptedException {
			  //处理文件名，做分割
			  FileSplit fileSplit = (FileSplit) context.getInputSplit();
			  String completeFileName = fileSplit.getPath().getName();
			  int dotIndex = getCharacterPosition(completeFileName);
			  String fileName = completeFileName.substring(0, dotIndex);
                //进行map的过程
			  String tempString = new String();
			  String lineString = value.toString().toLowerCase();
			  StringTokenizer itrStringTokenizer = new StringTokenizer(lineString);
			  while(itrStringTokenizer.hasMoreTokens()){
				  tempString = itrStringTokenizer.nextToken();
				  Text word = new Text();
				  word.set(tempString + "#" + fileName);//设置新的key
				  context.write(word, new IntWritable(1));
			  }
		  }
	  }

	  public static class SumCombiner extends  Reducer<Text,IntWritable,Text,IntWritable>{
		  private IntWritable resultIntWritable = new IntWritable();
		  public void reduce (Text key,Iterable<IntWritable> values,Context context)
				  throws IOException,InterruptedException {
			  int sum = 0;
			  for(IntWritable valIntWritable : values){
				  sum += valIntWritable.get();
			  }
			  resultIntWritable.set(sum);
			  context.write(key, resultIntWritable);
		  }
	  }

	  public static class NewPartitioner extends HashPartitioner<Text, IntWritable>{
		  public int getPartition(Text key,IntWritable value,int numReduceTasks){
			  String term = new String();
			  term  = key.toString().split(",")[0];
			  return super.getPartition(new Text(term), value, numReduceTasks);
		  }
	  }

	  public static class InvertedIndexReducer extends Reducer<Text,IntWritable, Text, Text> {
		  private Text word1 = new Text();
		  private Text word2 = new Text();
		  String temp = new String();
		  Text currentItem = new Text(" ");
		  ArrayList<String> postingList = new ArrayList<String>();
		  private int fileCount = 0;
  
		  //用于将词频保留两位小数的处理函数
		  public double round(double value,int scale,int roundingMode){
			  BigDecimal bdBigDecimal = new BigDecimal(value);
			  bdBigDecimal = bdBigDecimal.setScale(scale, roundingMode);
			  double d = bdBigDecimal.doubleValue();
			  bdBigDecimal = null;
			  return d;
		  }
    
		  public void reduce( Text key, Iterable<IntWritable> values, Context context )
    		throws IOException,InterruptedException {
			  //最后一个词语由cleanup函数输出
			  int sum = 0;
			  word1.set(key.toString().split("#")[0]);
			  temp = key.toString().split("#")[1];
			  for(IntWritable valIntWritable : values){
				  sum += valIntWritable.get();//把词语出现次数加起来
			  }
			  fileCount++;//文件个数统计
			  word2.set(temp + ":" + sum + ";");
			  if(!currentItem.equals(word1) && !currentItem.equals("")){
				  StringBuilder outBuilder = new StringBuilder();
				  int count = 0;
				  for(String p: postingList){
					  outBuilder.append(p);
					  count += Integer.parseInt(p.substring(p.indexOf(":")+1, p.indexOf(";")).trim());
				  }
				  StringBuilder outputStringBuilder = new StringBuilder();
				  Double average_count = round((double)((double)count/(double)fileCount),2,BigDecimal.ROUND_DOWN);
				  outputStringBuilder.append(outBuilder);
                       //round函数是自己定义的，为了把词频结果保留两位小数
				  wordArrayList.add(word1.toString());
				  wordArrayList.add(average_count.toString());
				  if(count > 0)
					  context.write(currentItem, new Text(outputStringBuilder.toString()));
				  fileCount = 0;
				  postingList = new ArrayList<String>();
			  }
			  currentItem = new Text(word1);
			  postingList.add(word2.toString());
		  }
		  //cleanup是为了最后一个词语的输出
		  public void cleanup(Context context) throws IOException,InterruptedException{
			  StringBuilder outBuilder = new StringBuilder();
			  int count = 0;
			  fileCount++;
			  for(String p : postingList){
				  outBuilder.append(p);
				  count += Integer.parseInt(p.substring(p.indexOf(":")+1, p.indexOf(";")).trim());
			  }
			  StringBuilder outputStringBuilder = new StringBuilder();
			  Double average_count = round((double)((double)count/(double)fileCount),2,BigDecimal.ROUND_DOWN);
			  outputStringBuilder.append(outBuilder);
			  wordArrayList.add(word1.toString());
			  wordArrayList.add(average_count.toString());
			  if(count > 0){
				  context.write(currentItem, new Text(outputStringBuilder.toString()));
			  }
		  }
	  } 
	  
	  @SuppressWarnings("deprecation")
	  public static void addData(String tableName,String rowKey,String family,String qualifier,String value)
			throws Exception{
		  try{
			  @SuppressWarnings("resource")
			  HTable table = new HTable(configuration, tableName);
			  Put put = new Put(Bytes.toBytes(rowKey));
			  put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
			  table.put(put);
//			  	System.out.println("insert record success!");
		  }catch(IOException e){
			  e.printStackTrace();
		  }	
	  }

	  @SuppressWarnings("deprecation")
	  public static void main(String[] args){
		  try {
			  Configuration conf = new Configuration();
//        	    DistributedCache.addCacheFile(new URI(""), conf);
			  Job job = new Job(conf);
			  job.setJarByClass(InvertedIndexerHbase.class);

			  job.setInputFormatClass(TextInputFormat.class);

			  job.setMapperClass(InvertedIndexMapper.class);
			  job.setCombinerClass(SumCombiner.class);
			  job.setReducerClass(InvertedIndexReducer.class);
			  job.setPartitionerClass(NewPartitioner.class);

			  job.setMapOutputKeyClass(Text.class);
         	  job.setMapOutputValueClass(IntWritable.class);

         	  job.setOutputKeyClass(Text.class);
         	  job.setOutputValueClass(Text.class);    

         	  FileInputFormat.addInputPath(job,new Path(args[0]));
         	  FileOutputFormat.setOutputPath(job,new Path(args[1]));
//         	  System.exit(job.waitForCompletion(true) ? 0 : 1);
         	  job.waitForCompletion(true);
         	  System.out.println("the size of wordArrayList:"+wordArrayList.size());
         	  for(int i = 0;i < wordArrayList.size();i += 2){
         		 addData("Wuxia",wordArrayList.get(i).toString(), "average", "average for every word", wordArrayList.get(i+1).toString());
         	  }
		  } catch (Exception e) {
			  e.printStackTrace();
		  }
	  }
}

