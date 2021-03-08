package advancetest1.mapreduce.task5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//5、找到“燃料种类”为空的列，并将其值填写为“汽油”。
class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("计数器", "燃料种类为空");
        String[] lines = value.toString().trim().split(",");
        String fuel = lines[15];
        if (fuel.equals("") || fuel.equals(null) || fuel.equals("NULL") || fuel.equals("NAN")) {
            fuel = "汽油";
            counter.increment(1L);
        }
        String line = "";
        for (int i = 0; i < 14; i++) {
            line += lines[i] + "\t";
        }
        line += fuel + "\t";
        for (int i = 15; i < lines.length; i++) {
            if (i == lines.length - 1) {
                line += lines[i];
            } else {
                line += lines[i] + "\t";
            }
        }
        context.write(new Text(line), NullWritable.get());
    }
}
