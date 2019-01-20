# TCore.Sql Interfaces
Provides a layer on top of `Sytem.Data.SqlClient` to ease things like reading records from a result set as well as building queries. (And scoping transactions and connections).
## SqlWhere
`TCore.SqlWhere` provides basic query building services.

Building queries can quickly become tedious when you have subqueries and aliases. `SqlWhere` provides alias support that allows you to define aliases via a mapping.
If you are interactively building a query and just need to create
`SqlWhere::AddAlias(string sTable)`

<!--stackedit_data:
eyJoaXN0b3J5IjpbLTU4NzU2ODcxMl19
-->