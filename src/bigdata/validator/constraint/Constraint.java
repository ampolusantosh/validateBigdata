package bigdata.validator.constraint;

import java.util.List;

import bigdata.validator.internal.ConstraintParser;

public abstract class Constraint {
	public String name;
	public String table;
	public String column;
	public int column_index;
	public String type;
	public String output; //<!-- records that satisfies this check goes to this output directory -->
	public List<String> additional_conditions;
	public List<String> constraint_detail;
	
	public Constraint(String name, String table, String column,
			int column_index, String type, String output,
			List<String> additional_conditions, List<String> constraint_detail) {
		super();
		this.name = name;
		this.table = table;
		this.column = column;
		this.column_index = column_index;
		this.type = type;
		this.output = output;
		this.additional_conditions = additional_conditions;
		this.constraint_detail = constraint_detail;
	}

	public abstract boolean apply(String columnValue);
	
	public void setup(ConstraintParser constraintConfig){}
	
	public String toString()
	{
		return "Constraint Name : "+name
		+" | type : "+type
		+" | table : "+table
		+" | column : "+column
		+" | column idx : "+column_index
		+" | output : "+output
		+" | Constraint Details : "+constraint_detail.toString()
		+" | Additional Condtn. : "+additional_conditions.toString();
	}
}