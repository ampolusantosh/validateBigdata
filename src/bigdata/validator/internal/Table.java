package bigdata.validator.internal;

import java.util.List;

public class Table {
	String name;
	String delimiter;
	List<String> columns;
	boolean ignoreHeader;
	String sourceDir;
			// Absolute source is inputRoot + sourceDir

	public Table(String name, String delimiter, List<String> columns,
			boolean ignoreHeader, String sourceDir) {
		super();
		this.name = name;
		this.delimiter = delimiter;
		this.columns = columns;
		this.ignoreHeader = ignoreHeader;
		this.sourceDir = sourceDir;
	}
	public String getName() {
		return name;
	}
	public String getDelimiter() {
		return delimiter;
	}
	public List<String> getColumns() {
		return columns;
	}
	public boolean isIgnoreHeader() {
		return ignoreHeader;
	}
	public String getSourceDir() {
		return sourceDir;
	}
	public String toString()
	{
		return " Name = "+name+" columns = "+columns.toString()
				+ " ignoreHeader = "+ignoreHeader + " sourceDir = "+sourceDir;
	}
	public int getColumnIndex(String columnName)
	{
		int i=0;
		for(String col: columns)
		{
			if (col.equalsIgnoreCase(columnName))
				return i;
			else i++;
		}
		return -1;
	}
}
