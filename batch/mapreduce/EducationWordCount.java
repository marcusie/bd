package xyz.hiubo.bigdata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EducationWordCount {

    public static class EducationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text education = new Text();
        private Set<String> allowedEducations = new HashSet<>();
        private boolean isHeader = true;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            allowedEducations.add("中专/中技");
            allowedEducations.add("初中及以下");
            allowedEducations.add("博士");
            allowedEducations.add("大专");
            allowedEducations.add("学历不限");
            allowedEducations.add("本科");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 使用更健壮的表头判断方式
            if (isHeader) {
                isHeader = false;
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length > 5) { // 确保有足够的字段
                String educationRequirement = fields[5]; // 学历要求是第六个字段
                if (allowedEducations.contains(educationRequirement)) {
                    education.set(educationRequirement);
                    context.write(education, one);
                }
            }
        }
    }

    public static class EducationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: EducationWordCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Education Word Count");
        job.setJarByClass(EducationWordCount.class);

        // 设置Mapper和Reducer
        job.setMapperClass(EducationMapper.class);
        job.setReducerClass(EducationReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
