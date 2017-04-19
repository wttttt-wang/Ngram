package com.wttttt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-18
 * Time: 15:11
 */
public class CountNgram {

    private static void getTopK(Iterable<Text> values, Queue<String> heap, int topNum){
        for (Text value : values){
            if (heap.size() < topNum) {
                heap.offer(value.toString());
            } else{
                heap.offer(value.toString());
                heap.poll();
            }
        }
    }

    public static class CountMapper extends
            org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        private int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            threshold = context.getConfiguration().getInt("threshold", 10);
        }

        /**
         * @Description: filter the record whose count < threshold
         * @param key: word1 word2 ... wordi
         * @param value: count
         * @output: key --> word1 word2 .. wordi-1 value --> wordi=count [count >= threshold]
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: v1 v2 ... vi\tcount
            String[] line = value.toString().trim().split("\t");
            String[] words = line[0].split("\\s+");
            int count = Integer.parseInt(line[1]);

            if (count < threshold || words.length < 2) return;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]);
                sb.append(" ");
            }

            context.write(new Text(sb.toString().trim()), new Text(words[words.length - 1]
                    + "=" + count));
            // output: key = v1 v2 ... vi-1  value = vi=count
        }
    }



    public static class CountReducer extends Reducer<Text, Text, Text, Text> {

        private int topNum;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topNum = context.getConfiguration().getInt("topNum", 5);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input:  key = v1 v2 ... vi-1   value = vi=count

            Queue<String> heap = new PriorityQueue<String>(topNum, ValueComparator);

            getTopK(values, heap, topNum);

            // output: key = v1 v2 ... vi-1   value = vi=count
            int size = heap.size();
            for (int i = 0; i < size; i++) {
                context.write(key, new Text(heap.poll()));
            }
        }

    }


    public static Comparator<String> ValueComparator = new Comparator<String>() {
        public int compare(String s1, String s2) {
            return Integer.parseInt(s1.split("=")[1].trim()) - Integer.parseInt(s2.split("=")[1].trim());
        }
    };

    // reducer for writing to mysql db
    public static class DBReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        private int topNum;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topNum = context.getConfiguration().getInt("topNum", 5);
        }


        /**
         * @Description: select top-k prediction for each orgin words[], using priorityQueue.
         *       Simple n-1 gram here. For n-n gram, we just need to plat a trick in mysql
         *       by using `select * from output where origin like ...`
         *
         * @Output: mysqlDBOutput[origin, predict, count]
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input:  key = v1 v2 ... vi-1   value = vi=count

            Queue<String> heap = new PriorityQueue<String>(topNum, ValueComparator);

            getTopK(values, heap, topNum);

            // output: key = v1 v2 ... vi-1   value = vi=count
            int size = heap.size();
            for (int i = 0; i < size; i++) {
                String[] right = heap.poll().split("=");
                context.write(new DBOutputWritable(key.toString(), right[0], Integer.parseInt(right[1])), NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
        // 1. command line parser
        String inputPath = args[0];
        String outputPath = args[1];
        int threshold = Integer.parseInt(args[2]);
        int topNum = Integer.parseInt(args[3]);


        Configuration conf2 = new Configuration();
        conf2.setInt("threshold", threshold);
        conf2.setInt("topNum", topNum);

        DBConfiguration.configureDB(conf2, "com.mysql.jdbc.Driver", "jdbc:mysql://10.3.242.98:3306/test",
                "root", "111111");

        Job job2 = Job.getInstance(conf2, "CountNgram");
        job2.setJarByClass(CountNgram.class);

        job2.setMapperClass(CountNgram.CountMapper.class);
        job2.setReducerClass(CountNgram.DBReducer.class);

        // combiner -->
        // same with reducer only when mapper and reducer are of same <key, value> type
        job2.setCombinerClass(CountNgram.CountReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);


        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);
        //(.., tableName, FiledNames)
        DBOutputFormat.setOutput(job2, "output", "origin", "predict", "count");

        TextInputFormat.setInputPaths(job2, new Path(inputPath));
        TextOutputFormat.setOutputPath(job2, new Path(outputPath));
        job2.waitForCompletion(true);

    }

}

