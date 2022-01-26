# Rdbtools

**This package is experimental, and not supported by the MoJ Analytical Platform Team, so use at own risk. The preferred method is to use the `botor` package, which is the package which will give you the highest chance of help on the AP support channels.**

## What is Rdbtools?

This is an extension of the `noctua` package, for interacting with AWS Athena.
See https://dyfanjones.github.io/noctua/reference/index.html

This provides a few convenience functions for MoJ users, and also extends the methods defined in the noctua package to allow users easy access to a safe temporary database for writing out data.

The existing package dbtools does the same thing, this way is just implemented all in R and doesn't require a Python dependency.

## Installing Rdbtools

Then install laload with one of the the following commands:

 - If using renv: `renv::install("moj-analytical-services/Rdbtools")`
 - If not using renv: `devtools::install_github("moj-analytical-services/Rdbtools")` (you may need to install devtools first)

You can use the same command to update the package, if it is changed on Github later.

## How to use

### Connecting a session and querying

See https://dyfanjones.github.io/noctua/reference/index.html for the full list of functions you can call to interact with Athena.

To query a database, use:

```
library(Rdbtools)
con <- connect_athena() # creates a connection with sensible defaults
data <- dbGetQuery(con, "SELECT * FROM database.table") # queries and puts data in R environment
dbDisconnect(con) # disconnects the connection
```

### The temporary database

Wherever you put the special string `__temp__` then this will refer to a database which is specific to your user and where you can write temporary tables before you read them out.
This works with both the noctua functions (which are updated in this package for connections made via `connect_athena()`) and the convenience functions (e.g. `read_sql()`).

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
 + dbWriteTable (but not the permission issue in the help for this function)

If there are further noctua/DBI function where the `__temp__` string substitution would be useful then open up an issue or pull request and the Rdbtools community can try and arrange an implementation.

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

### Single queries (deprecated)

The function `read_sql` is provided which replicates the same function from `dbtools` - this is kept for backwards compatibility only.
This creates a database connection, reads the data and then closes the connection every call.
If you want to do more than one call to Athena the method below is probably better.
Also note that since authentication has moved to WebIdentity then any new temporary tables created under one connection will only be accessible by that same connection, so `read_sql` cannot be used to read a table created by a another function unless the relevant connection object is supplied to the `con` argument (this is different to previous usage of `read_sql`.
