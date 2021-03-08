package advancetest2.mapreduce.task1.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String str = "";
        for (int i = 0; i < lines.length - 1; i++) {
            str = str + "," + lines[i];
        }

        str = str.substring(1);
        context.write(new Text(str), NullWritable.get());
    }

}
