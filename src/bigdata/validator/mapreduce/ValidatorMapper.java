package bigdata.validator.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import bigdata.validator.constraint.Constraint;
import bigdata.validator.internal.ConstraintParser;

public class ValidatorMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public static enum COUNTERS {
		LINE_NUMBER
	};
	String table;
	String delimiter;
	boolean ignoreHeader;
	
	public static final Log LOG = LogFactory.getLog(ValidatorMapper.class.getName());
	MultipleOutputs<NullWritable, Text> mout;
	String valid_output_dir_name, invalid_output_dir_name;
	List<Constraint> cons;
	@Override
	public void setup(Context context)
	{
		table=context.getConfiguration().get("table");
		// Parse Constraints object to get validator configurations
		ConstraintParser constraintConfig=new ConstraintParser();
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
			if(c.type.equalsIgnoreCase("FK"))
				c.cacheParent(constraintConfig);
		}
		mout=new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		context.getCounter(COUNTERS.LINE_NUMBER).increment(1);
    	long line_num=context.getCounter(COUNTERS.LINE_NUMBER).getValue();
    	if (line_num==1 && ignoreHeader)
    		return;
		String record[]=value.toString().split(delimiter);
		boolean violated=false;
		boolean violatedToInvalid=true;
		boolean condition=false;
		for ( Constraint c: cons )
		{
			condition=c.apply(record[c.column_index]);
			if (c.output.equalsIgnoreCase("valid_data_dir_name"))
				violatedToInvalid=true;
			else violatedToInvalid=false;
			if(( violatedToInvalid ^ condition ))
			{
				mout.write(NullWritable.get(),
						new Text(value.toString()+delimiter.charAt(delimiter.length()-1)
								+"VIOLATED Constraint : "+c.type+" for column : "+c.column
								),
						invalid_output_dir_name+"invalid_file");
				violated=true;
			}
		}
		if (!violated)
			mout.write(NullWritable.get(), value, valid_output_dir_name+"valid_file");
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mout.close();
	}
}
