package basictest6.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务1、统计每个url的访问数量
 * 输出结果举例：
 * POST  /service/1.htm  3  【本条数据表示：发送post请求访问/service/1.htm的次数为3（一个url可能会发送post请求，也可能会发送get请求）】
 * GET   /service/1.htm  2  【本条数据表示：发送get请求访问/service/1.htm的次数为2，（一个url可能会发送post请求，也可能会发送get请求）】
 * GET   /    4
 */
public class main {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test/output/output1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(main.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

