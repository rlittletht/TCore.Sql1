# TCore.Sql Interfaces
Provides a layer on top of `Sytem.Data.SqlClient` to ease things like reading records from a result set as well as building queries. (And scoping transactions and connections).
## SqlWhere
`TCore.SqlWhere` provides basic query building services.

Building queries can quickly become tedious when you have subqueries and aliases. `SqlWhere` provides alias support that allows you to define aliases via a mapping.
```
SELECT Foo.Data1, Foo.Data2, Bar.Data3
  FROM TableFoo AS Foo
  INNER JOIN TableBar Bar
    ON Foo.FooKey = Bar.BarKey
```
Consider the above query, but you dynamically want to construct the selection criteria. Your constant query might look like
```
static string s_s


If you are interactively building a query and just need to create an alias on-the-fly, use `SqlWhere::AddAlias(string sTable)`, which will create an alias unique to 

<!--stackedit_data:
eyJoaXN0b3J5IjpbODk3NTcyMzU2LC0xNDgyMDI2NTg1XX0=
-->