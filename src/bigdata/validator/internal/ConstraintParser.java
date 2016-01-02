package bigdata.validator.internal;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import bigdata.validator.constraint.Constraint;

public class ConstraintParser {
	
	HashMap<String, String> type_class=new HashMap<String, String>();	// Store the mapping of constraint type and class name
	/*
	Constraints are stored in the following :
	HashMap of Table and Constraints
	Constraints is a List which contains object of type Constraint
	*/
	public HashMap<String, List<Constraint>> allConstraints=new HashMap<String, List<Constraint>>();
	public HashMap<String, Table> allTables=new HashMap<String, Table>(); // Store table details such that it can be easily accessed using the name
	public String source_root_dir;
	public String output_root_dir;
	public String valid_data_dir_name;
	public String invalid_data_dir_name;

	public static final Log LOG = LogFactory.getLog(ConstraintParser.class.getName());
	
	public void parse(String ConfigXmlPath) throws Exception
	{
		File config=new File(ConfigXmlPath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(config);
		doc.getDocumentElement().normalize();
		
		// Read all defined classes for the constraints.
		String constraint_package=((Element)doc.getElementsByTagName("global").item(0))
				.getElementsByTagName("constraint_package")
				.item(0)
				.getTextContent();
		
		NodeList consClasses=((Element)doc.getElementsByTagName("global").item(0))
					.getElementsByTagName("constraint_class");
		for(int i=0;i<consClasses.getLength();i++)
		{
			type_class.put(((Element)consClasses.item(i)).getAttribute("type"),
					constraint_package+"."+((Element)consClasses.item(i)).getTextContent() );
		}
		
		// List of Constraint classes
		List<String> keys = new ArrayList<String>(type_class.keySet());
		for (String key: keys) {
		    LOG.info("Class for "+key + " : " + type_class.get(key));
		}
		// Classes defined for the constraints are now stored in type_class hash map
		
		// Read global block
		source_root_dir=((Element)doc.getElementsByTagName("global").item(0))
				.getElementsByTagName("source_root_dir")
				.item(0)
				.getTextContent();
		output_root_dir=((Element)doc.getElementsByTagName("global").item(0))
				.getElementsByTagName("output_root_dir")
				.item(0)
				.getTextContent();
		valid_data_dir_name=((Element)doc.getElementsByTagName("global").item(0))
				.getElementsByTagName("valid_data_dir_name")
				.item(0)
				.getTextContent();
		invalid_data_dir_name=((Element)doc.getElementsByTagName("global").item(0))
				.getElementsByTagName("invalid_data_dir_name")
				.item(0)
				.getTextContent();
		
		LOG.info("Source root dir : "+source_root_dir);
		LOG.info("Output root dir : "+output_root_dir);
		LOG.info("Valid data dir : "+valid_data_dir_name);
		LOG.info("Invalid data dir : "+invalid_data_dir_name);
		// Global block loaded
				
		// Read tables (data model)
		NodeList tables=doc.getElementsByTagName("data_model_table");
		//Temporary variables
		String name, delimiter, ignoreHeader, sourceDir;
		for(int i=0;i<tables.getLength();i++)
		{
			name= ((Element)tables.item(i)).getAttribute("name");
			delimiter=((Element)tables.item(i)).getElementsByTagName("delimiter")
					.item(0).getTextContent();
			ignoreHeader=((Element)tables.item(i)).getElementsByTagName("ignoreHeader")
					.item(0).getTextContent();
			sourceDir=((Element)tables.item(i)).getElementsByTagName("source")
					.item(0).getTextContent();
			
			allTables.put(name, 
					new Table(name, delimiter
					,Arrays.asList(((Element)tables.item(i)).getElementsByTagName("columns")
									.item(0).getTextContent()
									.split(delimiter))		
					,Boolean.parseBoolean(ignoreHeader)
					,sourceDir));
		}
		// List of Loaded tables
		keys = new ArrayList<String>(allTables.keySet());
		for (String key: keys) {
		    LOG.info("Table name : "+key + " : Details : " + allTables.get(key).toString());
		}
		
		
		// Reading constraints
		
		// Temporary variables
		name="";
		String table;
		String column;
		String output;
		int column_index;
		String type;
		List<String> additional_conditions;
		List<String> constraint_detail;
		
		LOG.info("Starting to process contraints");
		NodeList nConstraints=doc.getElementsByTagName("constraint");
		for(int i=0;i<nConstraints.getLength();i++)
		{
			additional_conditions=new ArrayList<String>();
			constraint_detail=new ArrayList<String>();
			name=((Element)nConstraints.item(i)).getAttribute("name");
			type=((Element)nConstraints.item(i)).getElementsByTagName("type")
					.item(0).getTextContent();
			table=((Element)nConstraints.item(i)).getElementsByTagName("table")
					.item(0).getTextContent();
			column=((Element)nConstraints.item(i)).getElementsByTagName("column")
					.item(0).getTextContent();
			output=((Element)nConstraints.item(i)).getElementsByTagName("output")
					.item(0).getTextContent();
			column_index=findColumnIndex(table, column);
			
			// Read additional conditions
			int n=((Element)nConstraints.item(i)).getElementsByTagName("additional_condition")
					.getLength(); // Number of additional conditions
			if ( n>0 ) {
				for(int j=0;j<n;j++)
					additional_conditions.add(((Element)nConstraints.item(i)).getElementsByTagName("additional_condition")
							.item(j).getTextContent());
			}
			
			// Read constraint details
			n=((Element)nConstraints.item(i)).getElementsByTagName("constraint_detail")
					.getLength(); // Number of additional conditions
			if ( n>0 ) {
				for(int j=0;j<n;j++)
					constraint_detail.add(((Element)nConstraints.item(i)).getElementsByTagName("constraint_detail")
							.item(j).getTextContent());
			}
			
			LOG.info("Processing : "+name
					+" | type : "+type
					+" | table : "+table
					+" | column : "+column
					+" | output : "+output
					+" | Constraint Details : "+constraint_detail.toString()
					+" | Additional Condtn. : "+additional_conditions.toString());
			
			
			// Wrap into a constraint object
			try {
				Class<?> c=Class.forName(type_class.get(type));
				Constructor<?> constructor=c.getConstructor(String.class, String.class, String.class, 
						int.class, String.class, String.class, List.class, List.class);
				Constraint cons=(Constraint) constructor.newInstance(name, table, column, column_index, type, output, additional_conditions, constraint_detail);
				
				if(allConstraints.get(table) == null)
					allConstraints.put(table, new ArrayList<Constraint>());
				allConstraints.get(table).add(cons);
			}
			catch(Exception e)	{
				LOG.error(e.toString());
			}
		}
		
		// List all loaded constraints from HashMap allConstraints
		keys = new ArrayList<String>(allConstraints.keySet());
		for (String key: keys) {
		    LOG.info("Constraints for table "+key + " : " + allConstraints.get(key).toString());
		}
		
	}
	public int findColumnIndex(String table, String columnName)
	{
		List<String> cols=allTables.get(table).columns;
		int i=0;
		for(String col: cols)
		{
			if (col.equalsIgnoreCase(columnName))
				return i;
			else i++;
		}
		return -1;
	}
}
