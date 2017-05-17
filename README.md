# outlier-detection
Model-based propositionalization for outlier detection

Transforms features learned with a relational BN model into a single table to be used in a standard single-table outlier detection method.

Input: 

1. A relational database
2. A target entity set (e.g. Movies in IMDB)
3. A learned Bayesian network (using [FactorBase](https://github.com/sfu-cl-lab/FactorBase).

Output: A single data table where each row represents a target instance and each column represents a conjunctive relational feature.
