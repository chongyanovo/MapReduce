package basictest8.task3.step1;

import basictest8.task3.ValueBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务3 将图书表、用户信息表、评价表合并为总表，并将总表放到目录total下
 * 商品名称           用户名称    评级   评论内容   评论日期             出版社           销售量
 * c++程序设计基础     我好怕怕       3      有些错别字   2020-8-20    电子工业出版社     4
 * java程序设计     你好怕怕       3级     内容太旧了   2021-9-2       电子工业出版社     3
 * 大数据技术基础     王春芳     4级     内容不错     2021-2-4         清华大学出版社      2
 * ……
 */
public class main {
    private static Path BookINPUT_PATH = new Path("hdfs://localhost:9000/book/book.csv");
    private static Path CommentINPUT_PATH = new Path("hdfs://localhost:9000/output/comment3");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/total/step1");

//    private static Path BookINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/book.csv");
//    private static Path CommentINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/comment3");
//    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/total/step1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(main.class);

        MultipleInputs.addInputPath(job, CommentINPUT_PATH, TextInputFormat.class, CommentMap.class);
        MultipleInputs.addInputPath(job, BookINPUT_PATH, TextInputFormat.class, BookMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

