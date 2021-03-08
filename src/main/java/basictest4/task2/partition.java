package basictest4.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

class partition extends Partitioner<Text, Bean> {
    @Override
    public int getPartition(Text text, Bean dataBean22, int i) {
        String sex = dataBean22.getSex();
        if (sex.equals("男")) {
            return 0;
        } else {
            return 1;
        }
    }
}
