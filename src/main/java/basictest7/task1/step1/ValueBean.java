package basictest7.task1.step1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ValueBean implements WritableComparable<ValueBean> {
    private String value;
    private int flag;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(ValueBean valueBean) {
        int result = this.value.compareTo(valueBean.getValue());
        if (result == 0) {
            result = this.flag - valueBean.getFlag();
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }
}
