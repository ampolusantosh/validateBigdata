package bigdata.validator.constraint;

import java.util.Arrays;
import java.util.List;

public class In extends Constraint {
	
	public In(String name, String table, String column, int column_index,
			String type, String output, List<String> additional_conditions,
			List<String> constraint_detail) {
		super(name, table, column, column_index, type, output, additional_conditions,
				constraint_detail);
	}

	@Override
	public boolean apply(String columnValue) {
		String set[]=constraint_detail.get(1).split(constraint_detail.get(0));
		Arrays.sort(set);
		if(Arrays.binarySearch(set, columnValue) <0 )
			return false;
		else return true;
	}

}
