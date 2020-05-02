package statistics.reducer;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;

import statistics.formula.FormulaComponent;
import statistics.formula.FormulaValue;

public class CountryAggregator implements Function2<Map<FormulaComponent, FormulaValue>, Map<FormulaComponent, FormulaValue>, Map<FormulaComponent, FormulaValue>> {

	@Override
	public Map<FormulaComponent, FormulaValue> call(Map<FormulaComponent, FormulaValue> firstMap, Map<FormulaComponent, FormulaValue> secondMap){
		firstMap.putAll(secondMap);
		return firstMap;
	}
}
