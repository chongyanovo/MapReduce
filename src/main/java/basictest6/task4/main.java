package basictest6.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务4、将每一个访问ip访问的url放到不同的目录下（目录名称为ip）
 * 输出结果举例：
 * 目录名称：172.18.35.1
 * 数据内容：
 * 172.18.35.1 - - [2013-03-04 23:38:27] "POST /service/1.htm HTTP/1.0" 200 1
 * 172.18.35.1 - - [2013-03-05 23:38:27] "POST / service/12.htm HTTP/1.0" 200  2
 * 172.18.35.1 - - [2013-03-09 23:38:27] "POST html/notes/1.html HTTP/1.0" 200 2
 * 172.18.35.1 - - [2013-04-04 23:38:27] "GET html/notes/2.html HTTP/1.0" 200 1
 */
public class main {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test/output/output4");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(main.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

