package basictest1.task3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Bean implements WritableComparable<Bean> {
    private int hot;
    private String location;

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return location + "\t" + hot;
    }

    @Override
    public int compareTo(Bean bean) {
        return bean.hot - this.hot;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(hot);
        dataOutput.writeUTF(location);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hot = dataInput.readInt();
        this.location = dataInput.readUTF();
    }
}
