package basictest7.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        //6,1,2,3,5
        String str = line[6] + "\t" + line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[5] + "\t";
        context.write(new Text(str), NullWritable.get());
    }
}
