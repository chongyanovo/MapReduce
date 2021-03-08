package advancetest2.mapreduce.task2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Bean implements WritableComparable<Bean> {
    private String good;
    private int sales;

    public String getGood() {
        return good;
    }

    public void setGood(String good) {
        this.good = good;
    }

    public int getSales() {
        return sales;
    }

    public void setSales(int sales) {
        this.sales = sales;
    }

    @Override
    public String toString() {
        return good + "\t" + sales;
    }

    @Override
    public int compareTo(Bean o) {
        int result = this.good.compareTo(o.getGood());
        if (result == 0) {
            result = o.getSales() - this.sales;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(good);
        dataOutput.writeInt(sales);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.good = dataInput.readUTF();
        this.sales = dataInput.readInt();
    }
}
