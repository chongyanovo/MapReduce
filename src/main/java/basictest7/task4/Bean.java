package basictest7.task4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Bean implements WritableComparable<Bean> {
    private String compare;
    private String year;

    public String getCompare() {
        return compare;
    }

    public void setCompare(String compare) {
        this.compare = compare;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return compare + "\t" + year;
    }

    @Override
    public int compareTo(Bean bean) {
        int result = this.compare.compareTo(bean.getCompare());
        if (result == 0) {
            result = this.year.compareTo(bean.getYear());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compare);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compare = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }
}
