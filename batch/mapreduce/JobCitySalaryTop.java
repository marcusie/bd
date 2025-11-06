package xyz.hiubo.bigdata;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobCitySalaryTop {

    // 第一阶段：计算各岗位在各城市的平均薪资
    public static class SalaryMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text jobCity = new Text();
        private Text salary = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头行
            if (key.get() == 0) {
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length >= 4) { // 确保有足够的字段
                try {
                    String city = fields[1];  // 城市在第二列
                    String job = fields[2];   // 岗位在第三列
                    double salaryValue = Double.parseDouble(fields[3]);  // 薪资在第四列
                    
                    jobCity.set(job + "\t" + city);
                    salary.set(String.valueOf(salaryValue));
                    context.write(jobCity, salary);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid salary value: " + value.toString());
                }
            }
        }
    }

    public static class SalaryReducer extends Reducer<Text, Text, Text, Text> {
        private Text job = new Text();
        private Text cityAvgSalary = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\t");
            String jobName = parts[0];
            String cityName = parts[1];
            
            double sum = 0;
            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }
            
            double avgSalary = sum / count;
            
            job.set(jobName);
            cityAvgSalary.set(cityName + "\t" + String.format("%.2f", avgSalary));
            context.write(job, cityAvgSalary);
        }
    }

    // 第二阶段：对每个岗位的城市按平均薪资排序并取Top5
    public static class Top5Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text job = new Text();
        private Text citySalary = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 3) {
                job.set(parts[0]);  // 岗位
                citySalary.set(parts[1] + "\t" + parts[2]);  // 城市和平均薪资
                context.write(job, citySalary);
            }
        }
    }

    public static class Top5Reducer extends Reducer<Text, Text, NullWritable, Text> {
        private Text output = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 使用TreeMap按平均薪资降序排序
            TreeMap<Double, List<String>> salaryToCities = new TreeMap<>(Collections.reverseOrder());
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                String city = parts[0];
                double salary = Double.parseDouble(parts[1]);
                
                salaryToCities.computeIfAbsent(salary, k -> new ArrayList<>()).add(city);
            }
            
            // 输出Top5
            int topCount = 0;
            for (Map.Entry<Double, List<String>> entry : salaryToCities.entrySet()) {
                double salary = entry.getKey();
                List<String> cities = entry.getValue();
                
                for (String city : cities) {
                    if (topCount >= 5) {
                        break;
                    }
                    
                    output.set(key.toString() + "\t" + city + "\t" + String.format("%.2f", salary));
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
            System.err.println("Usage: JobCitySalaryTop <input path> <temp path> <output path>");
            System.exit(2);
        }

        // 第一阶段作业：计算平均薪资
        Job job1 = Job.getInstance(conf, "JobCitySalaryAvg");
        job1.setJarByClass(JobCitySalaryTop.class);
        
        job1.setMapperClass(SalaryMapper.class);
        job1.setReducerClass(SalaryReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        
        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }

        // 第二阶段作业：取Top5
        Job job2 = Job.getInstance(conf, "JobCitySalaryTop5");
        job2.setJarByClass(JobCitySalaryTop.class);
        
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