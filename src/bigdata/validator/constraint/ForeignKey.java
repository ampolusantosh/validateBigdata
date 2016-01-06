package bigdata.validator.constraint;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigdata.validator.internal.ConstraintParser;

public class ForeignKey extends Constraint {

	private List<String> parentColumnValues;
	private String parentSourceDir;
	private String parentTableDelimiter;
	private int parentColumnIndex;
	private boolean parentTableIgnoreHeader;
	
	public static final Log LOG = LogFactory.getLog(ForeignKey.class.getName());
	
	public ForeignKey(String name, String table, String column,
			int column_index, String type, String output,
			List<String> additional_conditions, List<String> constraint_detail) {
		super(name, table, column, column_index, type, output,
				additional_conditions, constraint_detail);
	}

	@Override
	public boolean apply(String columnValue) {
		if( Collections.binarySearch(parentColumnValues, columnValue) <0 )
			return false;
		else return true;
	}
	
	// Cache the parent table.
	public void setup(ConstraintParser constraintConfig)
	{
		String parentTable=constraint_detail.get(0).split("\\.")[0];
		String parentColumn=constraint_detail.get(0).split("\\.")[1];
		
		// Find source dir and files of Parent table from constraintConfig object
		
		parentSourceDir=constraintConfig.source_root_dir
				+System.getProperty("file.separator")
				+constraintConfig.allTables.get(parentTable).getSourceDir();
		parentColumnIndex=constraintConfig.allTables.get(parentTable).getColumnIndex(parentColumn);
		parentTableDelimiter=constraintConfig.allTables.get(parentTable).getDelimiter();
		parentTableIgnoreHeader=constraintConfig.allTables.get(parentTable).isIgnoreHeader();
		parentColumnValues=new ArrayList<String>();
		final File source = new File(parentSourceDir);
		
		// Read values
		readValues(source);
		
		// Sort ArrayList
		Collections.sort(parentColumnValues);
	}
	private void readValues(final File source)
	{
		for (final File fileEntry : source.listFiles()) {
	        if (fileEntry.isDirectory()) {
	        	readValues(fileEntry);
	        } 
	        else {
	        	try {
	        		BufferedReader br=new BufferedReader(new FileReader(fileEntry));
	        		String line=br.readLine();
	        		if(parentTableIgnoreHeader) line=br.readLine(); // Ignoring the fist line
	        		while(line!=null) {
		        		parentColumnValues.add(line.split(parentTableDelimiter)[parentColumnIndex]);
		        		line=br.readLine();
	        		}
	        		br.close();
	        	}
	        	catch(Exception e)	{
					LOG.error(e.toString());
				}
	        }
	    }
	}
}
