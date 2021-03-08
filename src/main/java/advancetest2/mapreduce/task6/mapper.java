package advancetest2.mapreduce.task6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

//6、统计每一个商品的销售情况（每个商品对应一个文件名）
class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String good = lines[2];
        String str = good + "\t" + lines[1] + "\t" + lines[0] + "\t" + lines[3] + "\t" + lines[5];
        mos.write(str, NullWritable.get(), good);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}
