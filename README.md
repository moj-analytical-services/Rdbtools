# Rdbtools

**Please note that this is not officially supported by the AP team and is intended to be community supported.**

## What is Rdbtools?

This is an extension of the `noctua` package, for interacting with AWS Athena through the MoJ's analytical platform.
See https://dyfanjones.github.io/noctua/reference/index.html

The primary purpose of this package is to easily allow MoJ analysts to access data on Athena, without having to understand anything about the underlying authentication systems.
This access is provided through the R database interface [`DBI`](https://dbi.r-dbi.org/), and so works with the standard database functions used in R.
It also works with [`dbplyr`](https://dbplyr.tidyverse.org/), which is an extention of `dplyr` allowing you to use familiar tidyverse functions on data in Athena itself (reducing the need for large data pre-processing steps in R, and without having to learn SQL).

In addition, this package extends the methods defined in the noctua package to allow users easy access to a safe temporary database for intermediate processing steps.

The secondary purpose of this package is to provide backwards compatability with `dbtools` which does not work on the new AP infrastructure.
For this the package provides a few convenience functions for MoJ users.
The key difference with this package over `dbtools` is that it is implemented all in R and doesn't require a Python dependency.

# Installing Rdbtools

Then install Rdbtools with one of the the following commands:

 - If using renv:
     - install `Rdbtools`: `renv::install("moj-analytical-services/Rdbtools")` or if that doesn't work try `renv::install("git@github.com:moj-analytical-services/Rdbtools.git")`
 - If not using renv:
     - install `Rdbtools`: `devtools::install_github("moj-analytical-services/Rdbtools")` (you may need to install devtools first)

You can use the same command to update the package, if it is changed on Github later.

## Troubleshooting installation

`Rdbtools` requires recent versions of some of the underlying packages to function (e.g. `paws` and `noctua`).
If you get errors during installation, or `connect_athena` always returns an error saying "Something went wrong getting temporary credentials" then first try updating the CRAN mirror being used with the following command on the R console:

`options(repos = list(CRAN = "https://packagemanager.posit.co/cran/latest"))`

# How to use Rdbtools

## Basic connecting a session and querying

### With SQL commands (using DBI)

See https://dyfanjones.github.io/noctua/reference/index.html for the full list of functions you can call to interact with Athena.

To query a database, use:

```
library(Rdbtools)
con <- connect_athena() # creates a connection with sensible defaults
data <- dbGetQuery(con, "SELECT * FROM database.table") # queries and puts data in R environment
dbDisconnect(con) # disconnects the connection
```

### Using dbplyr

See https://dbplyr.tidyverse.org/index.html

As an example:
```
library(tidyverse)
library(dbplyr)
library(Rdbtools)

con <- connect_athena()
datadb <- tbl(con, sql("select * from database.name")) # create the dbplyr link
# use dplyr as usual on this dataframe link
datadb %>%
  filter(size < 10) %>%
  group_by() %>%
  summarise(n = n(),
            total = sum(total))

dbDisconnect(con) # disconnects the connection
```

Note that if you need any function within dbplyr which does a copy (e.g. joining a local table to a remote table)
then you need to ensure you have the right permissions for the staging directory you are using.
See the help page for `dbWriteTable` by running `?dbWriteTable` in the console.

## The temporary database

Each user can have a database which can store temporary tables.

Note that the tables created here will have their underlying data stored in the default staging directory
(which is different for each new connection) or that specified by the staging directory argument 
(which will remain the same for each new connection).
The permissions of the staging directory will determine who can access the data in the temporary tables.

### With SQL commands (using DBI)

Wherever you put the special string `__temp__` in SQL commands then this will refer to a database which is specific to your user and where you can write temporary tables before you read them out.
This works with the DBI functions (which are updated in this package for connections made via `connect_athena()`) and the convenience functions (e.g. `read_sql()`).

```
library(Rdbtools)
con <- connect_athena() # creates a connection with sensible defaults
dbExecute(con, "CREATE TABLE __temp__.name AS SELECT * FROM database.table") # queries and puts in temp space
data <- dbGetQuery(con, "SELECT * FROM __temp__.name") # queries and puts data in R environment
dbDisconnect(con) # disconnects the connection
```

The `__temp__` string substitution is implemented for:

 + dbGetQuery
 + dbExecute
 + dbGetTables
 + dbListTables
 + dbExistsTable
 + dbListFields
 + dbRemoveTable
 + dbWriteTable (but note the permission issue in the help for this function by running `?dbWriteTable` in the console)

If there are further noctua/DBI function where the `__temp__` string substitution would be useful then open up an issue or pull request and the Rdbtools community can try and arrange an implementation.

### Using dbplyr (or other packages)

The `__temp__` string is not understood by dbplyr functions, so to use the temporary database for this or other packages you have two options:

 + When creating the connection, you can specify the temporary database as the default schema: `connect_athena(schema_name = "__temp__")`. In this case dbplyr commands which do not specify a database will default to the temporary database (e.g. then `compute("temp_tbl"))` at the end of a dbplyr chain will create a table in the temporary database with the name "temp_tbl").
 + Alternatively, the `athena_temp_db` function will return a string with the name of the temporary database if required to manually create specific SQL commands, or in use in other functions not listed above.
 
The temporary database is the same each way, so you can mix dbplyr, DBI, and other packages in the same code.

## Advanced use

### The connection object

The connection object returned by `connect_athena()` contains all the information about a single authenticated session which allows access to the databases for which you have permission.
By default the authenticated session will last for one hour, after which you will have to create a new connection or else refresh your connection.
For most purposes creating a new connection will be sufficient, however you will lose access to any tables created in the `__temp__` database (as these are only accessible under the same session).
To refresh a connection, please use the `refresh_athena_connection()` function, or in a long script the `refresh_if_expired()` function may also be useful (see the help pages in RStudio for further details of these functions).



### The region argument when creating connection object

The region passed into the connect_athena() will be used for 
- Get temporary token for connecting athena service
- Run the query and store the query result to the staging dir

In order to run the query successfully, the region need to the region where the query will be run and query result will be stored in the staging dir. You can pass the value based on your case when calling connect_athena(), by default, the region will be decided based on serveral environment variables below:

- `AWS_ATHENA_QUERY_REGION`: An environment variable for specifying the region when the region where the query will be run is different from the default region from underlying running environment.

- `AWS_DEFAULT_REGION` and `AWS_REGION`: The default region which usually will be setup by the underlying running environment e.g. cluster, and they cannot be amended

othewise use `eu-west-1` as the default

In most cases, you do not need to worry about the region, the default region (`AWS_DEFAULT_REGION` and `AWS_REGION`) should be the one for running query and the one where your staging dir is.  When there is cross-region situation in your runnning environment and you want to save the time for passing the region every time when creating connection, you can use the `AWS_ATHENA_QUERY_REGION` to specify it. 

## Single queries (deprecated)

The function `read_sql` is provided which replicates the same function from `dbtools` - this is kept for backwards compatibility only.
This creates a database connection, reads the data and then closes the connection every call.
If you want to do more than one call to Athena the method below is probably better.
Also note that since authentication has moved to WebIdentity then any new temporary tables created under one connection will only be accessible by that same connection, so `read_sql` cannot be used to read a table created by a another function unless the relevant connection object is supplied to the `con` argument (this is different to previous usage of `read_sql`.
