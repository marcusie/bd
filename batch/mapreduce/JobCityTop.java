package xyz.hiubo.bigdata;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobCityTop {

    // 第一阶段：统计各岗位在各城市的招聘数量
    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text jobCity = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (key.get() == 0) {
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) { // 确保有足够的字段
                String job = fields[2]; // 假设岗位是第三列
                String city = fields[1]; // 假设城市是第二列
                jobCity.set(job + "\t" + city);
                context.write(jobCity, one);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, Text> {
        private Text job = new Text();
        private Text cityCount = new Text();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\t");
            String jobName = parts[0];
            String cityName = parts[1];
            
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }
            
            job.set(jobName);
            cityCount.set(cityName + "\t" + count);
            context.write(job, cityCount);
        }
    }

    // 第二阶段：对每个岗位的城市进行排序并取Top5
    public static class Top5Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text job = new Text();
        private Text cityCount = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                job.set(parts[0]); // 岗位
                cityCount.set(parts[1] + "\t" + parts[2]); // 城市+数量
                context.write(job, cityCount);
            }
        }
    }

    public static class Top5Reducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text output = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 使用TreeMap按数量降序排序
            TreeMap<Long, List<String>> countToCities = new TreeMap<>(Collections.reverseOrder());
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                String city = parts[0];
                long count = Long.parseLong(parts[1]);
                
                countToCities.computeIfAbsent(count, k -> new ArrayList<>()).add(city);
            }
            
            // 输出Top5
            int topCount = 0;
            for (Map.Entry<Long, List<String>> entry : countToCities.entrySet()) {
                long count = entry.getKey();
                List<String> cities = entry.getValue();
                
                for (String city : cities) {
                    if (topCount >= 5) {
                        break;
                    }
                    
                    output.set(key.toString() + "\t" + city + "\t" + count);
                    context.write(NullWritable.get(), output);
                    topCount++;
                }
                
                if (topCount >= 5) {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 3) {
            System.err.println("Usage: JobCityTop <input path> <temp path> <output path>");
            System.exit(2);
        }

        // 第一阶段作业：统计数量
        Job job1 = Job.getInstance(conf, "JobCityCount");
        job1.setJarByClass(JobCityTop.class);
        
        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(CountReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        
        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // 第二阶段作业：取Top5
        Job job2 = Job.getInstance(conf, "JobCityTop5");
        job2.setJarByClass(JobCityTop.class);
        
        job2.setMapperClass(Top5Mapper.class);
        job2.setReducerClass(Top5Reducer.class);
        
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
