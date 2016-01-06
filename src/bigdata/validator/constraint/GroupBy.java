package bigdata.validator.constraint;

import java.util.List;

import bigdata.validator.internal.Table;

public class GroupBy extends Constraint {

	public GroupBy(String name, String table, String column, int column_index,
			String type, String output, List<String> additional_conditions,
			List<String> constraint_detail) {
		super(name, table, column, column_index, type, output,
				additional_conditions, constraint_detail);
	}

	@Override
	public boolean apply(String columnValue) {
		// This should never called. 
		return true;
	}

	@Override
	public String getGroupByFields(String record, Table table)
	{
		String keys[]=constraint_detail.get(0).split(table.getDelimiter()); // This is list of columns
		String columns[]=record.split(table.getDelimiter());
		StringBuilder keyList=new StringBuilder();
		for(String key :keys )
		{
			keyList.append(columns[table.getColumnIndex(key)])
					.append(table.getDelimiter());
		}
		keyList.deleteCharAt(keyList.length()-1); // Remove last delimiter
		return keyList.toString();
	}
}
