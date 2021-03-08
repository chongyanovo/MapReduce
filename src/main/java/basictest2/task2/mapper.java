package basictest2.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    int count = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] line = value.toString().split(",");
//        String[] strs = {"分院", "专业", "班级", "课程名称", "班级人数", "平均成绩"};
        if ((str.indexOf("分院") == -1) && (str.indexOf("专业") == -1)
                && (str.indexOf("班级") == -1) && (str.indexOf("课程名称") == -1)
                && (str.indexOf("班级人数") == -1) && (str.indexOf("平均成绩") == -1)
        ) {
            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
        }
//        if (strs.equals(line)) {
//
//        }else {
//            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
//        }
//        if (count >= 1) {
//            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
//        }
//        count++;
    }
}
