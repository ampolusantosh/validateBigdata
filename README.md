# validate-bigdata
Validation framework to impose popular relational database constraints on BigData / Hive tables.

This framework can be used to add constraints configured using xml files on data in HDFS or any other file system where the data is present as flat files. The framework is called by the main class (bigdata.validator.Driver). It takes a set of csv/tsv files as input and outputs data into two folders, one which satisfies the contraints configured in the xml and the other which violates the constraints. The output is stored in the file system under different folders.

The processing happens using the Hadoop Map Reduce API, with all validations being done on the Map Side.
The framework is designed keeping in mind huge datasets managed by Hive tables using simple Text format.

Referential Integrity Constraint: Referential integrity constraints can be imposed by this framework on CSV/flat files or data files created by Hive Tables. This works by caching the parent table's column in memory, so the expected size of the column of the parent table which is referenced should not be very high.

Other simple constraints: Constraints like NOT NULL, BETWEEN, CHECK, etc can be imposed on the data and validation is done at the map side.

Future support: Adding unique constraint, constraints that require aggregate functions to be applied, etc.

