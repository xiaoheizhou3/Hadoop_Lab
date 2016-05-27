package hahha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class triangle {
  
  public static class sortMapper extends Mapper<Object, Text, Text, Text>{
      private Text a = new Text();
      private Text b = new Text();
      
      public void map(Object key,Text value,Context context)throws IOException,InterruptedException {

          StringTokenizer itr=new StringTokenizer(value.toString());
          long first = Integer.parseInt(itr.nextToken());
          long second = Integer.parseInt(itr.nextToken());
          if (first <= second){
              a.set(first+"#"+second);
              b.set("#");
          }
          else{
              a.set(second+"#"+first);
              b.set("#");
          }
          context.write(a, b);
      }
  }
  
  public static class sortReducer extends Reducer<Text, Text, Text, Text>{
      private Text b = new Text();    
      public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
          b.set("#");
          context.write(key, b);
      }
  }
  
  public static class findMap extends Mapper<LongWritable, Text, Text, Text>{
      public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException {
          StringTokenizer itr=new StringTokenizer(value.toString());
          String[] tokens = itr.nextToken().toString().split("#");
          String a = tokens[0];
          String b = tokens[1];
          Text left = new Text();
          Text right = new Text();
          left.set(a+"");
          right.set(b+"");
          context.write(left, right);
      }
  }
  
  public static class findReducer extends Reducer<Text, Text, Text, Text>{
      
       public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
           ArrayList<String> array = new ArrayList<String>();
           Text left = new Text();
           Text right = new Text();
           right.set("#");
           for(Text value : values){
               array.add(value.toString());
               left.set(key.toString()+"#"+value.toString());
               context.write(left, right);
           }
           for(int i=0; i<array.size(); i++){
               for(int j=i+1; j<array.size(); j++){
                   Text a = new Text();
                   Text b = new Text();
                   if(Integer.parseInt(array.get(i)) < Integer.parseInt(array.get(j))){
                       a.set(array.get(i)+"#"+array.get(j));
                       b.set("&");
                   }
                   else{
                       a.set(array.get(j)+"#"+array.get(i));
                       b.set("&");
                   }
                   context.write(a, b);
               }
           }
       }
  }
  
  public static class combineMap extends Mapper<LongWritable, Text, Text, Text>{
      private Text a = new Text();
      private Text b = new Text();
      public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException {
          StringTokenizer itr=new StringTokenizer(value.toString());
          a.set(itr.nextToken().toString());
          b.set(itr.nextToken().toString());
          context.write(a, b);
      }
  }
  
  public static class combineReducer extends Reducer<Text, Text, Text, Text>{
      private static int result = 0;
      
      public void cleanup(Context context) throws IOException, InterruptedException{
          context.write(new Text("Result: "), new Text(""+result));
      }
      
      public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException {
          int count = 0;
          int is_triangle = 0;
          for(Text value : values){
              if(value.toString().equalsIgnoreCase("#")){
                  is_triangle = 1;
              }else if(value.toString().equalsIgnoreCase("&")){
                  count ++;
              }else{
                  //System.out.println("wrong input number");
              }
          }
          if (is_triangle == 1){
              result += count;
          }
      }
  }
  
  public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length!=4) {
            System.err.println("Usage:invertedindex<in> <out1> <out2> <out3>");
            System.exit(2);
        }
        
        @SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "job1");
        job1.setJarByClass(triangle.class);
        job1.setMapperClass(sortMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(sortReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        
        job1.waitForCompletion(true);
        

        @SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "job2");
        job2.setJarByClass(triangle.class);
        job2.setMapperClass(findMap.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(findReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
        job2.waitForCompletion(job1.isComplete());
     
        @SuppressWarnings("deprecation")
		Job job3 = new Job(conf, "job3");
        job3.setJarByClass(triangle.class);
        job3.setMapperClass(combineMap.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(combineReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
        
        job3.waitForCompletion(job2.isComplete());

    }
  
}