# outlier-detection
Model-based propositionalization for outlier detection

Transforms features learned with a relational BN model into a single table to be used in a standard single-table outlier detection method.

Input: 

1. A relational database
2. A target entity set (e.g. Movies in IMDB)
3. A learned Bayesian network (using [FactorBase](https://github.com/sfu-cl-lab/FactorBase)).

Output: A single data table where each row represents a target instance and each column represents a conjunctive relational feature.


## How to Use - Overview

1. **Run Bayesian Network to get Contingency Tables**

     We provide [FactorBase](https://github.com/sfu-cl-lab/FactorBase) to make a Bayesian network that shows probabilistic dependencies between the relationships and attributes represented in the database. 
     
     Note that the default setting of the original contingency tables in source code is "unielwin_CT" database. So before compile you should modify the `scripts/setup.sql` to replace "unielwin_CT" with correct original table path.
        
        
2. **Point to the required database in your MySQL server**  

      Download the source code to your local computer.
      Modify `src/config.cfg` with your own configuration according to the sample format explained in the image.
      
      ![Sample Configuration](/FlatTable/src/images/configuration.png).
      
      The "dbname" represents the name of schema which stores the converted tables.
      
      The "column" represents the name of the column where the primary key is located.
      
      The "tablename" represents the name of the contingency table in original schema.
        
3. **Compile & Run** 

      + Go into `src` folder 
      + `javac -cp ".:./lib/*" Convert_table.java `  
      + `java -cp ".:./lib/*" Convert_table > result.txt `  
      
      Then you can check the converted table in your "dbname".

