
package test

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {

         /*这个函数是为了处理文件名字，以为所给的数据集里面文件名字都不一样，
           因此采用正则表达式匹配的方法来把文件名分割一下*/
    public static int getCharacterPosition(String string){
        Matcher slashMatcher = Pattern.compile("(\\.txt)|(\\.TXT)").matcher(string);
        if(slashMatcher.find()){
            return slashMatcher.start();
        }
        return 0;
    }

    protected void map( Object key, Text value,Context context)
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

class SumCombiner extends  Reducer<Text,IntWritable,Text,IntWritable>{
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

class NewPartitioner extends HashPartitioner<Text, IntWritable>{
    public int getPartition(Text key,IntWritable value,int numReduceTasks){
        String term = new String();
        term  = key.toString().split(",")[0];
        return super.getPartition(new Text(term), value, numReduceTasks);
    }
}

class InvertedIndexReducer extends Reducer<Text,IntWritable, Text, Text> {
    private Text word1 = new Text();
    private Text word2 = new Text();
    String temp = new String();
    static Text currentItem = new Text(" ");
    static ArrayList<String> postingList = new ArrayList<String>();
    private int fileCount = 0;
    //用于将词频保留两位小数的处理函数
    public double round(double value,int scale,int roundingMode){
        BigDecimal bdBigDecimal = new BigDecimal(value);
        bdBigDecimal = bdBigDecimal.setScale(scale, roundingMode);
        double d = bdBigDecimal.doubleValue();
        bdBigDecimal = null;
        return d;
    }

    protected void reduce( Text key, Iterable<IntWritable> values, Context context )
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
            outputStringBuilder.append(round((double)((double)count/(double)fileCount),2,BigDecimal.ROUND_DOWN) + "," + outBuilder);
                       //round函数是自己定义的，为了把词频结果保留两位小数
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
        outputStringBuilder.append(round((double)((double)count/(double)fileCount)\
                ,2,BigDecimal.ROUND_DOWN) + "," + outBuilder);
        if(count > 0){
            context.write(currentItem, new Text(outputStringBuilder.toString()));
        }
    }
} 

public class InvertedIndexer {
    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            @SuppressWarnings("deprecation")
            Job job = new Job(conf);
            job.setJarByClass(InvertedIndexer.class);

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
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
