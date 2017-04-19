package com.wttttt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-17
 * Time: 21:03
 */

public class SplitNgram {

    public static class SplitMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        int noGram;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("NoGram", 3);
        }

        /**
         *
         * @param key:
         * @param value: sentences split by '.'. Attention that there maybe
         *           several sentences inner splited by ?!... and so on.
         * @param context
         * @output For mapper: key --> ngram(i connective words, 2 <= i <= n)   value --> 1
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: This is my house, I love my house. (Maybe several sentences)
            // 1. remove numbers & toLowerCase
            String line = value.toString().trim().replaceAll("[0-9]"," ").toLowerCase();
            // 2. split it to several sentences
            String[] sentences = line.split("[\\pP\'\"]");

            for (String sentence : sentences) {
                // 3. remove all non-letters --> except space
                // 4. split each sentence to words
                String[] words = sentence.replaceAll("[^a-z\\s]", " ").trim().split("\\s+");

                if (words.length < 2) return;

                StringBuilder sb;
                for (int i = 0; i < words.length; i++) {
                    sb = new StringBuilder();
                    sb.append(words[i]);
                    for (int j = 1; j < noGram && i + j < words.length; j++) {
                        sb.append(" ");
                        sb.append(words[i + j]);
                        // output: (This is), (is my)... (This is my)
                        context.write(new Text(sb.toString().trim()), new IntWritable(1));
                    }
                }

            }


        }
    }




    public static class SplitReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        /**
         * @Description: simply count the occurance times for ngram
         * @output: key --> ngram(i connective words, 2 <= i <= n)   value --> count
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }



    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
        // 1. command line parser
        String inputPath = args[0];
        String outputPath = args[1];
        String noGram = args[2];

        Configuration conf = new Configuration();
        conf.set("noGram", noGram);

        // 因为在TextInputFormat的内部会读取配置，即
        // context.getConfiguration().get ("textinputformat.record.delimiter" ) ;
        conf.set("textinputformat.record.delimiter", ".");


        Job job1 = Job.getInstance(conf, "SplitNgram");
        job1.setJarByClass(SplitNgram.class);

        job1.setMapperClass(SplitMapper.class);
        job1.setReducerClass(SplitReducer.class);
        // combiner -->
        // same with reducer only when mapper and reducer are of same <key, value> type
        job1.setCombinerClass(SplitReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        // job1.setInputFormatClass(PatternInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        job1.waitForCompletion(true);

    }



}

