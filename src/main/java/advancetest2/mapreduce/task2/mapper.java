package advancetest2.mapreduce.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//2、按商家进行分区（每个商家对应一个文件夹），按销售额从高到低的顺序输出商品信息
class mapper extends Mapper<LongWritable, Text, Text, Bean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        Bean bean = new Bean();
        bean.setGood(lines[2]);
        int sum = Integer.parseInt(lines[3]) + Integer.parseInt(lines[5]);
        bean.setSales(sum);
        String shop = lines[1];
        context.write(new Text(shop), bean);
    }
}
