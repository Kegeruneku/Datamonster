package link.neolink.datamonster.example.sqltosql;

import link.neolink.datamonster.Datamonster;
import org.apache.hadoop.util.ToolRunner;

public class Main
{
   public static void main(String[] args) throws Exception
   {
      Datamonster sqlMonster = new Datamonster("sqlMonster");
      sqlMonster.setSQLCredentials(args[0], args[1], args[2]);
      sqlMonster.setWritables(DataWritable.class, DBInputWritable.class, DBOutputWritable.class);
      sqlMonster.mapperConfiguration("SQL QUERY HERE", Map.class);
      sqlMonster.reducerConfiguration("UPDATE", "out_table", new String[] {"output", "fields"}, new String[] {"conditional", "fields"}, Reduce.class);
      System.exit(ToolRunner.run(sqlMonster, args));
   }
}