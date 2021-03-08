package basictest7.task3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Bean implements WritableComparable<Bean> {
    private String compary;
    private String mouth;

    public String getCompary() {
        return compary;
    }

    public void setCompary(String compary) {
        this.compary = compary;
    }

    public String getMouth() {
        return mouth;
    }

    public void setMouth(String mouth) {
        this.mouth = mouth;
    }

    @Override
    public String toString() {
        return compary + "\t" + mouth;
    }

    @Override
    public int compareTo(Bean bean) {
        int result = this.compary.compareTo(bean.getCompary());
        if (result == 0) {
            result = this.mouth.compareTo(bean.getMouth());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compary);
        dataOutput.writeUTF(mouth);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compary = dataInput.readUTF();
        this.mouth = dataInput.readUTF();
    }
}
