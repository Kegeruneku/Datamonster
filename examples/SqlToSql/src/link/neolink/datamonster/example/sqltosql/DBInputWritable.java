package link.neolink.datamonster.example.sqltosql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable
{
   private Integer fieldName;

   public void readFields(DataInput in) throws IOException {}

   public void readFields(ResultSet rs) throws SQLException
   {
      fieldName = rs.getInt(1);
   }

   public void write(DataOutput out) throws IOException {}

   public void write(PreparedStatement ps) throws SQLException
   {
      ps.setInt(1, fieldName);
   }

   public Integer getField()
   {
      return fieldName;
   }
}