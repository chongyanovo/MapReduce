package basictest1.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class main {
    public static Path IN_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T3/input");
    public static Path OUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T3/output");
//    public static Path OUT_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T3/output");
//    public static Path IN_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T3/input");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T3");
        job.setJarByClass(main.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, IN_PATH);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem = OUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUT_PATH)) {
            fileSystem.delete(OUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

