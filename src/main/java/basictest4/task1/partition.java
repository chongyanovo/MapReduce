package basictest4.task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

class partition extends Partitioner<Bean, NullWritable> {
    @Override
    public int getPartition(Bean bean1, NullWritable nullWritable, int i) {
        //return (bean1.getArea().hashCode() & Integer.MAX_VALUE) % i;
        if (bean1.getArea().equals("武汉市")) {
            return 0;
        } else if (bean1.getArea().equals("黄石市")) {
            return 1;
        } else {
            return 2;
        }
    }
}
