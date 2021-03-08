package basictest8.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务1 用户信息表和评价表的“用户id”字段内容不规范，有些是Id-123，有些是Id_22332，请统一按“ID+编码”修改，比如：
 * Id-123 改为 ID123
 * Id_22332 改为ID22332
 * 要求：修改后的用户信息表放到目录user2，修改后的评价表放到目录comment2
 */
public class Question1 {
    private static Path UserINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/user/user.csv");
    private static Path CommentINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/comment/comment.csv");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo8/output");

//    private static Path UserINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/user.csv");
//    private static Path CommentINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/comment.csv");
//    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/out1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Question1.class);

        MultipleInputs.addInputPath(job, UserINPUT_PATH, TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, CommentINPUT_PATH, TextInputFormat.class, CommentMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(R1.class);
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

class UserMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().trim().split(",");
        String id = line[0].toUpperCase();
        id = id.replace("-", "");
        id = id.replace("_", "");
        String str = id + "," + line[1] + "," + line[2];
        context.write(new Text(str), NullWritable.get());
    }
}

class CommentMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String id = line[1].trim().toUpperCase();
        id = id.replace("-", "");
        id = id.replace("_", "");
        String str = line[0] + "," + id + "," + line[2] + "," + line[3] + "," + line[4];
        context.write(new Text(str), NullWritable.get());
    }
}

class R1 extends Reducer<Text, NullWritable, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String[] line = key.toString().trim().split(",");
        if (line.length == 3) {
            mos.write(key, NullWritable.get(), "user2/");
        } else {
            mos.write(key, NullWritable.get(), "comment2/");
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}