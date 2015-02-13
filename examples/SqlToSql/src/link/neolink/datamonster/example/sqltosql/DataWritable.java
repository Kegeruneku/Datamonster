package link.neolink.datamonster.example.sqltosql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable
{
   private Integer field = 0;

   @SuppressWarnings("unused")
   public DataWritable() {}

   public DataWritable(Integer field)
   {
      this.field = field;
   }

   @Override
   public void readFields(DataInput in) throws IOException
   {
      field = in.readInt();
   }

   @Override
   public void write(DataOutput out) throws IOException
   {
      out.writeInt(field);
   }

   public Integer getField()
   {
      return this.field;
   }
}

