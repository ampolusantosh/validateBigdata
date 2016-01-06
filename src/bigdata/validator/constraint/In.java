package bigdata.validator.constraint;

import java.util.Arrays;
import java.util.List;

import bigdata.validator.internal.ConstraintParser;

public class In extends Constraint {
	
	public In(String name, String table, String column, int column_index,
			String type, String output, List<String> additional_conditions,
			List<String> constraint_detail) {
		super(name, table, column, column_index, type, output, additional_conditions,
				constraint_detail);
	}
	String set[]=constraint_detail.get(1).split(constraint_detail.get(0));

	@Override
	public void setup(ConstraintParser constraintConfig) {
		Arrays.sort(set);
	}

	@Override
	public boolean apply(String columnValue) {
		if(Arrays.binarySearch(set, columnValue) <0 )
			return false;
		else return true;
	}

}
