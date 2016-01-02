package bigdata.validator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bigdata.validator.internal.ConstraintParser;
import bigdata.validator.mapreduce.ValidatorMapper;

public class Driver {
	public static final Log LOG = LogFactory.getLog(Driver.class.getName());
	
	public void run() throws Exception
	{
		ConstraintParser constraintConfig=new ConstraintParser();
		constraintConfig.parse();
		FileUtils.deleteDirectory(new File(constraintConfig.output_root_dir));
		List<String> tablesWithConstraints = new ArrayList<String>(constraintConfig.allConstraints.keySet());
		String inputPath;
		for (String table: tablesWithConstraints)
		{
			// Launch one Map Reduce Job for this table.
			
			Configuration conf = new Configuration();
			conf.set("table", table);
			
			Job job=new Job(conf);
			job.setJobName("MR data validation job for "+table);
//			job.setJarByClass(Driver.class); 
			
			inputPath=constraintConfig.source_root_dir+System.getProperty("file.separator")+constraintConfig.allTables.get(table).getSourceDir();
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(constraintConfig.output_root_dir+System.getProperty("file.separator")+constraintConfig.allTables.get(table).getSourceDir()));
			LOG.info("Added Input Path "+inputPath);
			LOG.info("Add root Output Path "+constraintConfig.output_root_dir+System.getProperty("file.separator")+constraintConfig.allTables.get(table).getSourceDir());
			
			 
			job.setMapperClass(ValidatorMapper.class);
			job.setNumReduceTasks(0);
			
			job.waitForCompletion(true);
			
//			System.exit(job.waitForCompletion(true) ? 0 : 1); 
			
		    
		}
		
	}
	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis();
		new Driver().run();
		LOG.info("Total time taken : "+(System.currentTimeMillis()-startTime)+" ms");
	
	}
}
