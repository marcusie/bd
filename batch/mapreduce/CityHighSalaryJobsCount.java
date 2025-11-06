package xyz.hiubo.bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CityHighSalaryJobsCount {

    // Mapper：筛选薪资>10k的记录，并按城市分组
    public static class HighSalaryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text city = new Text();
        private final LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头行
            if (key.get() == 0) {
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length >= 4) { // 确保有薪资字段
                try {
                    String cityName = fields[1];  // 城市在第二列
                    double salary = Double.parseDouble(fields[3]);  // 薪资在第四列
                    
                    // 筛选薪资>10k的记录
                    if (salary > 10.0) {
                        city.set(cityName);
                        context.write(city, one);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid salary value: " + value.toString());
                }
            }
        }
    }

    // Reducer：统计每个城市的符合条件职位数
    public static class HighSalaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: CityHighSalaryJobsCount <input path> <output path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "City High Salary Jobs Count");
        job.setJarByClass(CityHighSalaryJobsCount.class);
        
        job.setMapperClass(HighSalaryMapper.class);
        job.setReducerClass(HighSalaryReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
