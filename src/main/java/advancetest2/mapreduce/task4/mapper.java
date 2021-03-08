package advancetest2.mapreduce.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//4、找出南京市2月份销售量最高的是哪个地区
class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String area = lines[0];
        int sales = Integer.parseInt(lines[5]);
        context.write(new Text(area), new IntWritable(sales));
    }
}
