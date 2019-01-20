# TCore.Sql Interfaces
Provides a layer on top of `Sytem.Data.SqlClient` to ease things like reading records from a result set as well as building queries. (And scoping transactions and connections).
## SqlWhere
`TCore.SqlWhere` provides basic query building services.

Once created, you build up the Where clause using `Add`, `AddSubclause`, grouping, etc. When you are done constructing the where clause, you can get the full Where clause using `GetWhere`.
### Construction
You construct the where clause piece by piece. By default, the clause is empty.
#### SqlWhere.GetWhere
```
	SqlWhere.GetWhere(string sBase)
```

Given a base string (e.g. `"SELECT * FROM Table"`),  will return the entire, properly formatted clause.
If there are no conditions, then the WHERE clause will be completely omitted. NOTE: If there are any aliases in the base string (e.g. `"$$table_foo$$"`), they will be expanded to the proper alias.
#### SqlWhere.Add
```
	SqlWhere.Add(string s, Op op)
```
Adds the given condition to the clause. The condition is expected to be properly formed, and can contain aliases:
```
	sw.Add("$$tbl_foo$$.Value1 = 'match'", Op.And);
```
Valid enum values for `Op` are `And, AndNot, Or, OrNot`.
### SqlSelect
There are situations when you want to represent an entire T-SQL SELECT statement with an object. Typically this is because you want to use this as a clause in another select statement, or this could be used to represent the entire SELECT statement you are building.
#### SqlSelect
```
	TCore.SqlSelect(string sBaseQuery, Dictionary <string, string> mpAliases);
```
Create the object by providing the base query and the (optional) mapping of aliases.
#### AddOrderBy
```
	// SqlSelect.AddOrderBy(string sOrderBy)

	sqlSelect.AddOrderBy("$$tbl_foo$$.OrderKey ASC");
```
Adds the given order by (syntax determined by T-SQL) string. Aliases are supported.
#### Where
```
	// SqlSelect.Where

	sqlSelect.Where.Add("$$tbl_foo$$.Value1 = 'match'", Op.And);
```
The Where property returns the Where clause for the SqlSelect. This allows you to build up the query clause.
#### ToString
```
	/// SqlSelect.ToString()

	sql.CommandText = sqlSelect.ToString();
```
This returns the entire SQL select clause (including 
### Aliases
Building queries can quickly become tedious when you have subqueries and aliases. `SqlWhere` provides alias support that allows you to define aliases via a mapping.
```
	SELECT Foo.Data1, Foo.Data2, Bar.Data3
	  FROM TableFoo AS Foo
	  INNER JOIN TableBar Bar
	    ON Foo.FooKey = Bar.BarKey
```
Consider the above query, but you dynamically want to construct the selection criteria. Your constant query might look like
```
static string s_sSelectBase = "SELECT Foo.Data1, Foo.Data2, Bar.Data3 FROM TableFoo AS Foo INNER JOIN TableBar Bar ON Foo.FooKey = Bar.BarKey";
```
but later in code, when you go to append the query string, you have to manage all those aliases. Even worse, what if you have shared code that wants to generate the condition. You have to enforce the same aliases throughout your code.
`SqlWhere` allows you to use the underlying table name as a constant (`"$$TableFoo$$"`) and remaps dynamically to a given (or generated) alias.
#### AddAlias
`SqlWhere.AddAlias(string sTableName)` takes a table name and returns  an alias unique to the `SqlWhere` clause:
```
	SqlWhere sw = new SqlWhere();
	string s = sw.AddAlias("TableFoo"); // s == "S0"
	string s2 = sw.AddAlias("TableBar"); // s2 == "S1"
	string s3 = sw.AddAlias("TableFoo"); // s3 == "S2"
```
Note that subsequent duplicate requests will yield new aliases. This is essentially unsupported
#### AddAliases
More likely, you will want to define a bunch of aliases upfront in the code and just use those in the clauses. You can statically define a set of aliases and add them all at once with `AddAliases`
```
        public static Dictionary<string, string> s_mpAliases = new Dictionary<string, string>
        {
            { "TableFoo", "Foo" },
            { "TableBar", "Bar" }
        };
        ...
        SqlWhere sw = new SqlWhere();
        sw.AddAliases(s_mpAliases);
```

<!--stackedit_data:
eyJoaXN0b3J5IjpbLTE0MTY0NzEzODEsLTUyMDUyNTI3NCw5Nj
A3MjY2NjQsLTE0ODIwMjY1ODVdfQ==
-->