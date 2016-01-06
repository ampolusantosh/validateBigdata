package bigdata.validator.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bigdata.validator.constraint.Constraint;
import bigdata.validator.internal.ConstraintParser;


public class GroupByMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static enum COUNTERS {
		PROCESSED_RECORDS,
	};
	String table;
	String delimiter;
	boolean ignoreHeader;
	
	public static final Log LOG = LogFactory.getLog(ValidatorMapper.class.getName());
	List<Constraint> cons;
	String valid_output_dir_name, invalid_output_dir_name;
	ConstraintParser constraintConfig=new ConstraintParser();
	@Override
	public void setup(Context context)
	{
		table=context.getConfiguration().get("table");
		// Parse Constraints object to get validator configurations
		try {
			constraintConfig.parse(context.getConfiguration().get("ConfigXmlPath"));
			LOG.info("Constraints for table "+table + " : " + constraintConfig.allConstraints.get(table).toString());
			valid_output_dir_name=constraintConfig.valid_data_dir_name+System.getProperty("file.separator")
//					+constraintConfig.allTables.get(table).getSourceDir()
					+System.getProperty("file.separator");
			invalid_output_dir_name=constraintConfig.invalid_data_dir_name+System.getProperty("file.separator")
//					+constraintConfig.allTables.get(table).getSourceDir()
					+System.getProperty("file.separator");
		} catch (Exception e) {
			LOG.error(e.toString());
		}
		cons=constraintConfig.allConstraints.get(table);
		delimiter=constraintConfig.allTables.get(table).getDelimiter();
		ignoreHeader=constraintConfig.allTables.get(table).isIgnoreHeader();
		
		// For all constraints that are of type FK, call cacheParent function
		for ( Constraint c: cons )
		{
			c.setup(constraintConfig);
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		context.getCounter(COUNTERS.PROCESSED_RECORDS).increment(1);
    	long line_num=context.getCounter(COUNTERS.PROCESSED_RECORDS).getValue();
    	if (line_num==1 && ignoreHeader)
    		return;
		for( Constraint c: cons)	{
			String groupingFields="";
			groupingFields=c.getGroupByFields(value.toString(), constraintConfig.allTables.get(c.table));
			if (c.type.equalsIgnoreCase("GROUP_BY") )	{
				context.write (new Text(c.name+delimiter+groupingFields), value);
			}
		}
	}
}


// Add group by reducer
// add additional constraint support