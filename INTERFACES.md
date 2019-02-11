# TCore.Sql Interfaces
Provides a layer on top of `Sytem.Data.SqlClient` to ease things like reading records from a result set as well as building queries. (And scoping transactions and connections).
## Breaking Changes
Breaking changes from 1.1.0.0 to 1.2.0.0:
<None>
Breaking changes from 1.0.0.0 to 1.1.0.0:
* `SqlWhere.AddOrderBy()` moved to `SqlSelect.AddOrderBy()`
* `SqlWhere.AddInnerJoin()` moved to `SqlSelect.AddInnerJoin()`
* `SqlWhere.AddOrderBy()` now automatically generates "ORDER BY" T-SQL command text. Callers must no longer specify literal text "ORDER BY" when they call `AddOrderBy`
## Sample Usage
```
	Dictionary<string, string> mpAliases = new Dictionary<string, string>
	{
		{ "tbl_foo", "FOO" },
		{ "tbl_bar", "BAR" }
	};

	string sBaseQuery = "SELECT $$tbl_foo$$.FooValue, $$tbl_bar$$.BarValue FROM $$#tbl_foo$$";
	
	SqlSelect sqls = new SqlSelect(sBaseQuery, mpAliases);

	if (fMatchByMatch1)
		sqls.Where.Add("$$tbl_foo$$.Match1 = 'Match1'", Op.And);
		
	if (fMatchByMatch2)
	{
		sqls.Where.StartGroup(Op.Or);
		sqls.Where.Add("$$tbl_foo$$.Match2_1 = 'M1'", Op.And);
		sqls.Where.Add("$$tbl_foo$$.Match2_2 = 'M2'", Op.And);
		sqls.Where.EndGroup();
	}
	
	SqlWhere swInnerJoin = new SqlWhere();
	swInnerJoin.AddAliases(mpAliases);
	swInnerJoin.Add("$$tbl_foo$$.FooKey = $$tbl_bar$$.BarKey", Op.And);
	sqls.AddInnerJoin("$$#tbl_bar$$", swInnerJoin);

	sqlCommand.CommandText = sqls.ToString();
```

This will correctly deal with the existence of either of the clauses, with proper AND/OR usage, creating a correct syntax. This will also properly include the InnerJoin. 
**NOTE: Package 1.1.0.0 implemented AddInnerJoin on SqlSelect**
## SqlSelect
`Tcore.SqlSelect` provides a full SQL select statement, bringing together `SqlWhere`, `InnerJoin`, and decorations like `ORDER BY`, `GROUP BY`, `AS`, etc.

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
Valid enum values for `Op` are `And, AndNot, Or, OrNot`. **NOTE**: the first added clause is assumed to be required and should be `Op.And`.
#### StartGroup / EndGroup
```
	// SqlWhere.StartGroup(Op op)
	// SqlWhere.EndGroup()

	sw.StartGroup(Op.Or);
	sw.Add("$$tbl_foo$$.Value1 = 'match1'", Op.And);
	sw.Add("$$tbl_foo$$.Value2 = 'match2'", Op.And);
	sw.EndGroup()
```
Allows grouping of criteria to allow proper order of operation evaluation.
#### AddSubclause
```
	// SqlWhere.AddSubclause(string sFormat, SqlSelect select, Op op)

	sw.AddSubclause("$$tbl_foo$$.MyKey in {0}", select, Op.And)
```
Takes an already created `SqlSelect` object and adds that as a subclause. In this example, the given SELECT statement returns a set of records, so the subclause we are added checks to see if 
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
#### AddGroupBy
```
	// SqlSelect.AddGroupBy(string sGroupBy)

	sqlSelect.AddOrderBy("$$tbl_foo$$.GroupKey");
```
Adds the given Group By (syntax determined by T-SQL) string.

**NYI_NOTE: This is improperly implemented today on the SqlWhere object -- this has to be moved to here**
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
This returns the entire SQL select clause.

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
eyJoaXN0b3J5IjpbLTY0NTAyNjYwMSwtMTU5NDIxNzI1M119
-->