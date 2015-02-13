package link.neolink.datamonster.example.sqltosql;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Map extends Mapper<LongWritable, DBInputWritable, Text, DataWritable>
{
   protected void map(LongWritable id, DBInputWritable data, Context context)
   {
      try
      {
         context.write(new Text(String.valueOf(data.getField())), new DataWritable(data.getField()));
      }
      catch(IOException e)
      {
         e.printStackTrace();
      }
      catch(InterruptedException e)
      {
         e.printStackTrace();
      }
   }
}