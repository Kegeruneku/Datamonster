package link.neolink.datamonster.example.sqltosql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable
{
   private Integer field;

   public DBOutputWritable(Integer field)
   {
      this.field = field;
   }

   public void readFields(DataInput in) throws IOException {}

   public void readFields(ResultSet rs) throws SQLException
   {
      field = rs.getInt(1);
   }

   public void write(DataOutput out) throws IOException {}

   public void write(PreparedStatement ps) throws SQLException
   {
      // Output fields
      ps.setInt(1, field);
      ps.setInt(2, field);

      // Conditional fields
      ps.setInt(3, field);
      ps.setInt(4, field);
   }
}
