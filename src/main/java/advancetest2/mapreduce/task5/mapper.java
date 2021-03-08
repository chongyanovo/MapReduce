package advancetest2.mapreduce.task5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//5、找出南京市1月销售量最高的是哪个商品
class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String good = lines[2];
        int sales = Integer.parseInt(lines[3]);
        context.write(new Text(good), new IntWritable(sales));
    }
}
