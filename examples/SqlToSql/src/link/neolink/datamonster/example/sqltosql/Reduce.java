package link.neolink.datamonster.example.sqltosql;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

public class Reduce extends Reducer<Text, DataWritable, DBOutputWritable, NullWritable>
{
   protected void reduce(Text key, Iterable<DataWritable> values, Context context)
   {
      ArrayList<DataWritable> fields = new ArrayList<DataWritable>();

      // Just dumb copy of the values into field
      for(DataWritable value : values)
      {
         fields.add(new DataWritable(value.getField()));
      }

      // Add values in SQL
      try
      {
         for (DataWritable element : fields)
         {
            context.write(new DBOutputWritable(element.getField()), NullWritable.get());
         }
      }
      catch(IOException e)
      {
         System.err.println(e.getMessage());
         e.printStackTrace();
      }
      catch(InterruptedException e)
      {
         e.printStackTrace();
      }
   }
}
