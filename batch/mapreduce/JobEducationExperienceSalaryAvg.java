package xyz.hiubo.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JobEducationExperienceSalaryAvg {

    // 自定义组合键：岗位、学历和经验
    public static class JobEducationExperienceKey implements WritableComparable<JobEducationExperienceKey> {
        private Text job;
        private Text education;
        private Text experience;

        public JobEducationExperienceKey() {
            this.job = new Text();
            this.education = new Text();
            this.experience = new Text();
        }

        public JobEducationExperienceKey(Text job, Text education, Text experience) {
            this.job = job;
            this.education = education;
            this.experience = experience;
        }

        public void set(Text job, Text education, Text experience) {
            this.job = job;
            this.education = education;
            this.experience = experience;
        }

        public Text getJob() {
            return job;
        }

        public Text getEducation() {
            return education;
        }

        public Text getExperience() {
            return experience;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            job.write(out);
            education.write(out);
            experience.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            job.readFields(in);
            education.readFields(in);
            experience.readFields(in);
        }

        @Override
        public int compareTo(JobEducationExperienceKey other) {
            int jobCompare = job.compareTo(other.job);
            if (jobCompare != 0) {
                return jobCompare;
            }
            
            int educationCompare = education.compareTo(other.education);
            if (educationCompare != 0) {
                return educationCompare;
            }
            
            return experience.compareTo(other.experience);
        }

        @Override
        public int hashCode() {
            return job.hashCode() * 163 * 163 + education.hashCode() * 163 + experience.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobEducationExperienceKey that = (JobEducationExperienceKey) o;
            return job.equals(that.job) && 
                   education.equals(that.education) && 
                   experience.equals(that.experience);
        }

        @Override
        public String toString() {
            return job + "\t" + education + "\t" + experience;
        }
    }

    // Mapper：提取岗位、学历、经验和薪资信息
    public static class SalaryMapper extends Mapper<LongWritable, Text, JobEducationExperienceKey, DoubleWritable> {
        private JobEducationExperienceKey key = new JobEducationExperienceKey();
        private DoubleWritable salary = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头行
            if (key.get() == 0) {
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length >= 5) { // 确保有足够的字段
                try {
                    String job = fields[2];          // 岗位在第三列
                    String education = fields[5];    // 学历在第六列
                    String experience = fields[4];   // 经验在第五列
                    double salaryValue = Double.parseDouble(fields[3]); // 薪资在第四列
                    
                    this.key.set(new Text(job), new Text(education), new Text(experience));
                    salary.set(salaryValue);
                    context.write(this.key, salary);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid salary value: " + value.toString());
                }
            }
        }
    }

    // Combiner：在Map端进行局部聚合，减少数据传输
    public static class SalaryCombiner extends Reducer<JobEducationExperienceKey, DoubleWritable, JobEducationExperienceKey, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(JobEducationExperienceKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    // Reducer：计算平均薪资
    public static class SalaryReducer extends Reducer<JobEducationExperienceKey, DoubleWritable, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void reduce(JobEducationExperienceKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avgSalary = sum / count;
            
            outputKey.set(key.getJob() + "\t" + key.getEducation() + "\t" + key.getExperience());
            outputValue.set(String.format("%.2f", avgSalary));
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: JobEducationExperienceSalaryAvg <input path> <output path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Job Education Experience Salary Average");
        job.setJarByClass(JobEducationExperienceSalaryAvg.class);
        
        job.setMapperClass(SalaryMapper.class);
        job.setCombinerClass(SalaryCombiner.class);
        job.setReducerClass(SalaryReducer.class);
        
        job.setMapOutputKeyClass(JobEducationExperienceKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

