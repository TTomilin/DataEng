# Q4 Data Engineering 

## Milestone 1 — choosing and preparing your data, introductory analysis

For the first milestone, you are asked to:
- Choose, download, load, and clean one or more datasets, where you expect to find
correlations. The dataset(s) should include at least 5.000 vectors (preferably over 10.000 if the
vectors are low-dimensional).
- Build and implement an architecture that gets as input your dataset(s) and a correlation
measure, and finds the pairs with the high pairwise correlations (over a threshold τ ). The value
of τ depends on the dataset and the correlation measures. Set a value such that you return
around 10 pairs. Your code should not consider the trivial solutions (i.e., the pair that each
vector forms with itself, which has a maximum correlation).
- Implement two different correlation measures (verify with a teacher if these are not Pearson and
mutual information), and test your code with these measures.

### Constraints
1. The methods for computing the correlation measure should implement the following generic
interface (you can slightly modify it if you have a good reason):
double correlationFunction(dataType in1, dataType in2)
dataType can be a Spark RDD, dataframe, or dataset.
2. The Spark code should take the correlation function (the ones you defined, and anything else
that satisfies the generic interface) as an input parameter.
3. When
the correlation measure is commutative, your code should avoid redundant
computations, i.e., double comparisons.
4. Make sure that your Spark code scales out and avoids bottlenecks.
