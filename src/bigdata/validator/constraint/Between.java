package bigdata.validator.constraint;

import java.util.List;

public class Between extends Constraint {

	public Between(String name, String table, String column, int column_index,
			String type, String output, List<String> additional_conditions,
			List<String> constraint_detail) {
		super(name, table, column, column_index, type, output, additional_conditions,
				constraint_detail);
	}
	
	@Override
	public boolean apply(String columnValue) {
		if(	columnValue.compareTo(constraint_detail.get(0))<0 
				&& columnValue.compareTo(constraint_detail.get(1))>0	) 
			return true;
		else
			return false;
	}
}
