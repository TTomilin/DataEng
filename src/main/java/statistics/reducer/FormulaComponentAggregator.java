package statistics.reducer;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

public class FormulaComponentAggregator implements Function2<Map<FormulaComponentType, FormulaComponentValue>, Map<FormulaComponentType, FormulaComponentValue>, Map<FormulaComponentType, FormulaComponentValue>> {

	@Override
	public Map<FormulaComponentType, FormulaComponentValue> call(Map<FormulaComponentType, FormulaComponentValue> firstMap, Map<FormulaComponentType, FormulaComponentValue> secondMap){
		firstMap.putAll(secondMap);
		return firstMap;
	}
}
