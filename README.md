# Q4 Data Engineering 

## Instructions for running

1. Import the pom.xml as a Maven project in the IDE
2. Install Java SE Development Kit 11 and set it as the project SDK for compilation and JVM for execution
3. Install Maven for dependency management and have proper (default) local configuration in usr/m2/settings.xml
4. Enable "Annotation Processing" for the Lombok library under Project Settings -> Build -> Compiler 
(the program will execute without it, but give errors at compile time)

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
```java
double correlationFunction(dataType in1, dataType in2)
```
`dataType` can be a Spark RDD, dataframe, or dataset.

2. The Spark code should take the correlation function (the ones you defined, and anything else
that satisfies the generic interface) as an input parameter.
3. When
the correlation measure is commutative, your code should avoid redundant
computations, i.e., double comparisons.
4. Make sure that your Spark code scales out and avoids bottlenecks.

## Milestone 2 - multiple correlations
For the second milestone, you are asked to find high multiple correlations of size p , i.e.,
correlations between at least p ≥ 3 vectors, that are over a threshold τ . You are free to choose
the value of p , or set it as an input parameter of your code. As correlation measures, you should
consider (at least) the following two:
- Total correlation
- Pearson correlation, where the vectors of the two sides are computed by linear combinations (e.g., averaged).

You also need to define an aggregation function. The functionality of the aggregation function is (possibly) to reduce the number of input time series, e.g., by averaging as in the previous example.

### Constraints
1. You should define aggregation and correlation functions separately.
2. The aggregation function should implement one of the following generic interfaces (you can
slightly modify it if you have a good reason):
```java
dataType aggrFunction(List<dataType> in) //e.g., for averaging
List<dataType> aggrFunction(List<dataType> in) //e.g., the identity function
```

3. The correlation function should implement one of the following generic interfaces (you can
slightly modify it if you have a good reason):
```java
double correlationFunction(aggFunction(List<dataType> in))
double correlationFunction(aggFunction(List<dataType> in1), aggFunction(List<dataType> in2))
```

4. The Spark code should take the correlation and aggregation function (any implementations
that satisfy the described interface) as input parameters.
5. Make sure that you write Spark code that scales out and avoids bottlenecks.
6. When the correlation/aggregation functions are commutative and associative, you should
avoid redundant computations. This includes, e.g., computing avg(x,y)=avg(y,x) twice, or
computing corr((x,y), (z,w))=corr((w,z), (y,x))=corr((w,z), (x,y))=... multiple times.
