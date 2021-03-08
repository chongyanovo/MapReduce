package basictest4.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class main {
    public static Path INPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test4/T2/input");
    public static Path OUTPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test4/T2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T22");
        job.setJarByClass(main.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH2);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean.class);

        //job.setPartitionerClass(P22.class);

        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Bean.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setNumReduceTasks(2);

        FileSystem fileSystem = OUTPUT_PATH2.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH2)) {
            fileSystem.delete(OUTPUT_PATH2, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

