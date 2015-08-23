using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace TCore
{
	// ===============================================================================
	//  I  Q U E R Y  R E S U L T 
	// ===============================================================================
	public interface IQueryResult
	{
		bool FAddResultRow(SqlReader sqlr, int iRecordSet);
	}

    public class SqlSelect
    {
        private string m_sBase;
        private SqlWhere m_sw;
        private string m_sOrderBy;

        public override string ToString()
        {
            return String.Format("{0} {1}", m_sw.GetWhere(m_sBase), m_sOrderBy == null ? "" : m_sw.ExpandAliases(m_sOrderBy));
        }

        public SqlSelect(string sBase)
        {
            m_sBase = sBase;
            m_sw = new SqlWhere();
        }

        public SqlSelect(string sBase, Dictionary<string, string> mpAliases)
        {
            m_sBase = sBase;
            m_sw = new SqlWhere();
            m_sw.AddAliases(mpAliases);
        }

        public SqlSelect()
        { 
            m_sw = new SqlWhere();
        }

        public void AddBase(string sBase)
        {
            m_sBase = sBase;
        }

        public void AddAliases(Dictionary<string, string> mpAliases)
        {
            m_sw.AddAliases(mpAliases);
        }

        public void AddOrderBy(string sOrderBy)
        {
            m_sOrderBy = sOrderBy;
        }

        public SqlWhere Where { get { return m_sw; } }
    }
	// ===============================================================================
	//  B H  S Q L  I N N E R  J O I N
	// ===============================================================================
	public class SqlInnerJoin
	{
        string m_sJoin;
        SqlWhere m_sw;

		public override string ToString()
		{
			return String.Format("INNER JOIN {0} ON {1}", m_sJoin, m_sw.GetClause());
		}

		public SqlInnerJoin(string sJoin, SqlWhere swJoin)
		{
			m_sJoin = sJoin;
			m_sw = swJoin;
		}
	}

	// ===============================================================================
	//  S Q L  W H E R E
	// ===============================================================================
	public class SqlWhere
	{
		Dictionary<string, string> m_mpAliases = new Dictionary<string, string>();
		List<SqlInnerJoin> m_plij = new List<SqlInnerJoin>();
		List<Clause> m_plc;
		string m_sGroupBy;

		public enum Op
			{
			And,
			AndNot,
			Or,
			OrNot
			}

		public struct Clause
			{
			public Op op;
			public string sClause;
		    public SqlSelect sqlsSubclause;
			}


		/* A D D  A L I A S */
		/*----------------------------------------------------------------------------
			%%Function: AddAlias
			%%Qualified: BhSvc.SqlWhere.AddAlias
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public string AddAlias(string sTable)
		{
			string sAlias = String.Format("S{0}", m_mpAliases.Count);

			m_mpAliases.Add(sAlias, sTable);
			return sAlias;
		}

		/* A D D  A L I A S E S */
		/*----------------------------------------------------------------------------
			%%Function: AddAliases
			%%Qualified: BhSvc.SqlWhere.AddAliases
			%%Contact: rlittle

			add a collection of aliases			
		----------------------------------------------------------------------------*/
		public void AddAliases(Dictionary<string, string> mpAliases)
		{
			foreach (string sKey in mpAliases.Keys)
				{
				AddAlias(sKey, mpAliases[sKey]);
				}
		}

		/* A D D  A L I A S */
		/*----------------------------------------------------------------------------
			%%Function: AddAlias
			%%Qualified: BhSvc.SqlWhere.AddAlias
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public string AddAlias(string sTable, string sAlias)
		{
			m_mpAliases.Add(sAlias, sTable);

			return sAlias;
		}


		/* C L E A R */
		/*----------------------------------------------------------------------------
			%%Function: Clear
			%%Qualified: BhSvc.SqlWhere.Clear
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void Clear()
		{
			m_plc = new List<Clause>();
		}

		/* B H  S Q L  W H E R E */
		/*----------------------------------------------------------------------------
			%%Function: SqlWhere
			%%Qualified: BhSvc.SqlWhere.SqlWhere
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public SqlWhere()
		{
			Clear();
		}

		/* S T A R T  G R O U P */
		/*----------------------------------------------------------------------------
			%%Function: StartGroup
			%%Qualified: BhSvc.SqlWhere.StartGroup
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void StartGroup(Op op)
		{
			Clause c;

			c.op = op;
			c.sClause = "(";
		    c.sqlsSubclause = null;

			m_plc.Add(c);
		}

		/* E N D  G R O U P */
		/*----------------------------------------------------------------------------
			%%Function: EndGroup
			%%Qualified: BhSvc.SqlWhere.EndGroup
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void EndGroup()
		{
			Clause c;

			c.op = Op.And;
			c.sClause = ")";
		    c.sqlsSubclause = null;

			m_plc.Add(c);
		}

		/* L O O K U P  A L I A S */
		/*----------------------------------------------------------------------------
			%%Function: LookupAlias
			%%Qualified: BhSvc.SqlWhere.LookupAlias
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public string LookupAlias(string sTable)
		{
			foreach (System.Collections.DictionaryEntry de in (System.Collections.IDictionary)m_mpAliases)
				{
				if (String.Compare((string)de.Value, sTable) == 0)
					return(string)de.Key;
				}

			return null;
		}

		/* E X P A N D  A L I A S E S */
		/*----------------------------------------------------------------------------
			%%Function: ExpandAliases
			%%Qualified: BhSvc.SqlWhere.ExpandAliases
			%%Contact: rlittle
		 
		    given alias: "TBLO" -> "tblOuter"
		 
		    $$tblOuter$$ becomes "TBLO"
		    $$#tblOuter$$ becomes "tblOuter TBLO"
		----------------------------------------------------------------------------*/
		public string ExpandAliases(string s)
		{
			if (m_mpAliases == null || m_mpAliases.Keys.Count == 0)
				return s;

			int ich;

			while ((ich = s.IndexOf("$$")) != -1)
				{
				int ichLast = s.IndexOf("$$", ich + 2);

				if (ichLast == -1)
					return s;

				int ichAdjust = s.Substring(ich + 2, 1) == "#" ? 1 : 0;
				ich += ichAdjust;
				string sTable = s.Substring(ich + 2, ichLast - ich - 2);
				string sAlias = LookupAlias(sTable);

				if (sAlias == null)
					throw new Exception(String.Format("table {0} has no alias registered", sTable));

				s = s.Replace("$$#"+sTable+"$$", String.Format("{0} {1}", sTable, sAlias));
				s = s.Replace("$$"+sTable+"$$", sAlias);
				}
			return s;
		}

		/* A D D  I N N E R  J O I N */
		/*----------------------------------------------------------------------------
			%%Function: AddInnerJoin
			%%Qualified: BhSvc.SqlWhere.AddInnerJoin
			%%Contact: rlittle

			add an inner join			
		----------------------------------------------------------------------------*/
		public void AddInnerJoin(SqlInnerJoin ij)
		{
			m_plij.Add(ij);
		}

		/* A D D  G R O U P  B Y */
		/*----------------------------------------------------------------------------
			%%Function: AddGroupBy
			%%Qualified: BhSvc.SqlWhere.AddGroupBy
			%%Contact: rlittle

			add a group by clause, expanding aliases.			
		----------------------------------------------------------------------------*/
		public void AddGroupBy(string s)
		{
			s = ExpandAliases(s);

			m_sGroupBy = s;
		}

		/* A D D */
		/*----------------------------------------------------------------------------
			%%Function: Add
			%%Qualified: BhSvc.SqlWhere.Add
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void Add(string s, Op op)
		{
			Clause c;

			c.op = op;
			c.sClause = s;
		    c.sqlsSubclause = null;

			m_plc.Add(c);
		}

        public void AddEx(string s, Op op)
        {
            s = ExpandAliases(s);

            Add(s, op);
        }

        public void AddSubclause(string sFormat, SqlSelect sqls, Op op)
        {
            Clause c;

            c.op = op;
            c.sClause = sFormat;
            c.sqlsSubclause = sqls;

            m_plc.Add(c);
        }

		/* G E T  W H E R E */
		/*----------------------------------------------------------------------------
			%%Function: GetWhere
			%%Qualified: BhSvc.SqlWhere.GetWhere
			%%Contact: rlittle
		 
		    get the entire clause, combining the give base and any already set
		    group by
		----------------------------------------------------------------------------*/
		public string GetWhere(string sBase)
		{
			if (sBase == null)
				sBase = "";

			StringBuilder sb = new StringBuilder(256);

			sb.Append(ExpandAliases(sBase));

			if (m_plij != null)
				{
				foreach (SqlInnerJoin ij in m_plij)
					{
					sb.Append(" ");
					sb.Append(ExpandAliases(ij.ToString()));
					}
				}

			if (m_plc.Count == 0)
				return sb.ToString();

			sb.Append(" WHERE ");
			sb.Append(GetClause());

			if (m_sGroupBy != null)
				{
				sb.Append(" GROUP BY ");
				sb.Append(ExpandAliases(m_sGroupBy));
				}

			return sb.ToString();
		}


		/* G E T  C L A U S E */
		/*----------------------------------------------------------------------------
			%%Function: GetClause
			%%Qualified: tw.twsvc:SqlWhere.GetClause
			%%Contact: rlittle

			Just get the where portion....don't include WHERE
		----------------------------------------------------------------------------*/
		public string GetClause()
		{
			if (m_plc.Count == 0)
				return "";
//			else if (m_plc.Count == 1)
//				return ExpandAliases(String.Format("{0}", m_plc[0].sClause));
			else
				{
				bool fSkipOp = true;
				bool fSkipGroupClose = false;

				string sWhere = "";

				for (int i = 0; i < m_plc.Count; i++)
					{
					Clause c = m_plc[i];
					string sClause = c.sClause;
					string sOp;
					string sUnary = "";

                    if (c.sqlsSubclause != null)
                        {
                        sClause = String.Format(c.sClause, String.Format("({0})", c.sqlsSubclause.ToString()));
                        }
					if (c.op == Op.And)
						{
						sOp = "AND";
						}
					else if (c.op == Op.AndNot)
						{
						sOp = "AND";
						sUnary = " NOT ";
						}
					else if (c.op == Op.Or)
						{
						sOp = "OR";
						}
					else if (c.op == Op.OrNot)
						{
						sOp = "OR";
						sUnary = " NOT ";
						}
					else
						{
						sOp = "";
						}

					if (sClause == "(")
						{
						// simple skip of ()...
						if (i + 1 >= m_plc.Count || m_plc[i + 1].sClause != ")")
							{
							if (fSkipOp)
								{
								sWhere += sUnary + "(";
								fSkipOp = false;
								}
							else
								sWhere += String.Format(" {0}{1} (", sOp, sUnary);;

							fSkipOp = true;
							}
						else
							fSkipGroupClose	= true;
						}
					else if (sClause == ")")
						{
						if (!fSkipGroupClose)
							{
							sWhere += ") ";
							fSkipOp = false;
							}
						fSkipGroupClose = false;
						}
					else
					    {
					    if (m_plc.Count == 1)
					        fSkipOp = true;

                        sWhere += String.Format(" {0}{1} {2}", fSkipOp ? "" : sOp, sUnary, sClause);
						fSkipOp = false;
						}
					}
				return ExpandAliases(sWhere);
				}
		}


	    [TestFixture]
	    public class SqlUnitTest
	    {
	        [Test]
	        public void AliasUnitTest()
	        {
	            SqlWhere sw;
	            string sExpected;
	            sw = new SqlWhere();

	            string sUserAlias = sw.AddAlias("tw_user");
	            sw.AddAlias("tw_locks", "TWL");

	            sw.Add("$$tw_user$$.LockID = $$tw_locks$$.LockID", Op.And);
	            sExpected = "base WHERE   S0.LockID = TWL.LockID";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }

	        [Test]
	        public void GroupUnitTest_UnaryOperator()
	        {
	            string sExpected;
	            SqlWhere sw;

	            sw = new SqlWhere();
	            sw.Add("A=1", Op.And);
	            sw.StartGroup(Op.AndNot);
	            sw.StartGroup(Op.And);
	            sw.EndGroup();
	            sw.Add("B=1", Op.Or);
	            sw.EndGroup();
	            sw.Add("C=1", Op.And);

	            sExpected = "base WHERE   A=1 AND NOT  (  B=1)  AND C=1";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }

            [Test]
            public void SelectUnitTest()
            {
                String sExpected = "SELECT foo FROM bar WHERE   baz=boo ";
                SqlSelect sqls = new SqlSelect("SELECT foo FROM bar");

                sqls.Where.Add("baz=boo", Op.And);
                Assert.AreEqual(sExpected, sqls.ToString());
            }

            [Test]
            public void SelectUnitTest_OrderBy()
            {
                String sExpected = "SELECT foo FROM bar WHERE   baz=boo ORDER BY foo DESC";
                SqlSelect sqls = new SqlSelect("SELECT foo FROM bar");

                sqls.AddOrderBy("ORDER BY foo DESC");
                sqls.Where.Add("baz=boo", Op.And);
                Assert.AreEqual(sExpected, sqls.ToString());
            }

            [Test]
            public void SelectUnitTest_Aliases()
            {
                String sExpected = "SELECT AI.foo FROM alias1 AI WHERE   AI.baz=boo ";
                Dictionary<string, string> mpAliases = new Dictionary<string, string>
                    {
                    {"alias1", "AI"},
                    {"alias2", "AII"},
                    };
                SqlSelect sqls = new SqlSelect("SELECT $$alias1$$.foo FROM $$#alias1$$", mpAliases);
	
                sqls.Where.Add("$$alias1$$.baz=boo", Op.And);
                Assert.AreEqual(sExpected, sqls.ToString());
            }

            [Test]
            public void SelectUnitTest_OrderByAliases()
            {
                String sExpected = "SELECT AI.foo FROM alias1 AI WHERE   AI.baz=boo ORDER BY AI.foo DESC";
                Dictionary<string, string> mpAliases = new Dictionary<string, string>
                    {
                    {"alias1", "AI"},
                    {"alias2", "AII"},
                    };
                SqlSelect sqls = new SqlSelect("SELECT $$alias1$$.foo FROM $$#alias1$$", mpAliases);

                sqls.AddOrderBy("ORDER BY $$alias1$$.foo DESC");
                sqls.Where.Add("$$alias1$$.baz=boo", Op.And);
                Assert.AreEqual(sExpected, sqls.ToString());
            }
            
            [Test]
            public void SubclauseUnitTest()
            {
                string sExpected = "base WHERE   A=(SELECT foo FROM bar WHERE   baz=boo )";
                SqlSelect sqlsInner = new SqlSelect("SELECT foo FROM bar");
                SqlWhere sqlwOuter = new SqlWhere();

                sqlsInner.Where.Add("baz=boo", Op.And);
                sqlwOuter.AddSubclause("A={0}", sqlsInner, Op.And);

                Assert.AreEqual(sExpected, sqlwOuter.GetWhere("base"));
            }

            [Test]
            public void SubclauseUnitTest_Aliases()
            {
                string sExpected = "SELECT A1O.foo FROM alias1 A1O WHERE   A1O.foo=(SELECT A1I.foo FROM alias1 A1I WHERE   A1I.baz=boo ) ";
                Dictionary<string, string> mpAliasesInner = new Dictionary<string, string>
                    {
                    {"alias1", "A1I"},
                    };
                Dictionary<string, string> mpAliasesOuter = new Dictionary<string, string>
                    {
                    {"alias1", "A1O"},
                    };
                
                SqlSelect sqlsInner = new SqlSelect("SELECT $$alias1$$.foo FROM $$#alias1$$", mpAliasesInner);
                SqlSelect sqlsOuter = new SqlSelect("SELECT $$alias1$$.foo FROM $$#alias1$$", mpAliasesOuter);

                sqlsInner.Where.Add("$$alias1$$.baz=boo", Op.And);
                sqlsOuter.Where.AddSubclause("$$alias1$$.foo={0}", sqlsInner, Op.And);

                Assert.AreEqual(sExpected, sqlsOuter.ToString());
            }

	        [Test]
	        public void GroupUnitTest()
	        {
	            string sExpected;
	            SqlWhere sw;

	            sw = new SqlWhere();
	            sw.Add("A=1", Op.And);
	            sw.StartGroup(Op.And);
	            sw.StartGroup(Op.And);
	            sw.EndGroup();
	            sw.Add("B=1", Op.Or);
	            sw.EndGroup();
	            sw.Add("C=1", Op.And);

	            sExpected = "base WHERE   A=1 AND (  B=1)  AND C=1";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }
            
            [Test]
	        public void GroupUnitTest_GroupAtEnd()
	        {
	            string sExpected;
	            SqlWhere sw;

	            sw = new SqlWhere();
	            sw.Add("A=1", Op.And);
	            sw.StartGroup(Op.And);
	            sw.StartGroup(Op.And);
	            sw.EndGroup();
	            sw.Add("B=1", Op.Or);
	            sw.EndGroup();

                sExpected = "base WHERE   A=1 AND (  B=1) ";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }

	        [Test]
	        public void GroupUnitTests_NestedGroups()
	        {
	            string sExpected;
	            SqlWhere sw;

	            sw = new SqlWhere();

	            sw.Add("A=1", Op.And);
	            sw.Add("B=2", Op.And);
	            sw.StartGroup(Op.And);
	            sw.StartGroup(Op.And);
	            sw.Add("B1=2", Op.And);
	            sw.Add("B2=2", Op.And);
	            sw.EndGroup();
	            sw.StartGroup(Op.Or);
	            sw.Add("C1=2", Op.And);
	            sw.Add("C2=2", Op.And);
	            sw.EndGroup();
	            sw.StartGroup(Op.OrNot);
	            sw.Add("C1=2", Op.And);
	            sw.Add("C2=2", Op.And);
	            sw.EndGroup();
	            sw.EndGroup();
	            sw.Add("C=3", Op.And);

	            sExpected =
	                "base WHERE   A=1 AND B=2 AND ((  B1=2 AND B2=2)  OR (  C1=2 AND C2=2)  OR NOT  (  C1=2 AND C2=2) )  AND C=3";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }

	        [Test]
	        public void BasicOperatorUnitTest()
	        {
	            string sExpected;
	            SqlWhere sw = new SqlWhere();

	            sw.Add("A=1", Op.And);
	            sw.Add("B=2", Op.Or);
	            sw.Add("C=3", Op.And);

	            sExpected = "base WHERE   A=1 OR B=2 AND C=3";
	            Assert.AreEqual(sExpected, sw.GetWhere("base"));
	        }

            [Test]
	        /* I N N E R  J O I N  U N I T  T E S T */
	        /*----------------------------------------------------------------------------
                %%Function: InnerJoinUnitTest
                %%Qualified: BhSvc.SqlWhere.InnerJoinUnitTest
                %%Contact: rlittle

                Test the inner join functionality...
		 
                Table structure:
                Order Table:  <idOrder>, <idCustomer>, <idProduct>
                Cust Table: <idCustomer>, <sCustName>, <idCity>
                Prod Table: <idProduct>, <sProdName>
                City Table: <idCity>, <sCityName>, <sMayor>

            ----------------------------------------------------------------------------*/
	        public void InnerJoinUnitTest()
	        {
	            SqlWhere sw;
	            string sExpected;
	            string sBase;
	            string sTest;

	            // first query test -- just a readable form:
	            // idOrder, sCustName, sCityName, sProdName

	            // select Order.idOrder, Cust.sCustName, City.sCityName, Prod.sProdName

	            Dictionary<string, string> mpAliases = new Dictionary<string, string>
	                {
	                    {"tblOrder", "TBLO"},
	                    {"tblCust", "TBLC"},
	                    {"tblCity", "TBLCY"},
	                    {"tblProd", "TBLP"}
	                };

	            sBase =
	                "select $$tblOrder$$.idOrder, $$tblCust$$.sCustName, $$tblCity$$.sCityName, $$tblProd$$.sProdName from $$#tblOrder$$";
	            sExpected = "select TBLO.idOrder, TBLC.sCustName, TBLCY.sCityName, TBLP.sProdName from tblOrder TBLO " +
	                        "INNER JOIN tblCust TBLC ON   TBLC.idCust=TBLO.idCust " +
	                        "INNER JOIN tblCity TBLCY ON   TBLCY.idCity=TBLC.idCity " +
	                        "INNER JOIN tblProd TBLP ON   TBLP.idProd=TBLO.idProd";

	            sw = new SqlWhere();
	            sw.AddAliases(mpAliases);
	            SqlWhere swIJ;
	            swIJ = new SqlWhere();
	            swIJ.Add("$$tblCust$$.idCust=$$tblOrder$$.idCust", Op.And);
	            sw.AddInnerJoin(new SqlInnerJoin("$$#tblCust$$", swIJ));

	            swIJ = new SqlWhere();
	            swIJ.Add("$$tblCity$$.idCity=$$tblCust$$.idCity", Op.And);
	            sw.AddInnerJoin(new SqlInnerJoin("$$#tblCity$$", swIJ));

	            swIJ = new SqlWhere();
	            swIJ.Add("$$tblProd$$.idProd=$$tblOrder$$.idProd", Op.And);
	            sw.AddInnerJoin(new SqlInnerJoin("$$#tblProd$$", swIJ));

	            sTest = sw.GetWhere(sBase);
	            Assert.AreEqual(sExpected, sTest);

	            // second test, matching all the orders that relate to Mayor "dudley"...
	            //
	            // idOrder, scustName, sCityName, sProdName
	            // WHERE tblcy.sMayer == 'dudley'

	            // since this is the same inner join/select as before, with just some
	            // WHERE criteria, we'll build on top of bhsw

	            // same sBase as before
	            sExpected = "select TBLO.idOrder, TBLC.sCustName, TBLCY.sCityName, TBLP.sProdName from tblOrder TBLO " +
	                        "INNER JOIN tblCust TBLC ON   TBLC.idCust=TBLO.idCust " +
	                        "INNER JOIN tblCity TBLCY ON   TBLCY.idCity=TBLC.idCity " +
	                        "INNER JOIN tblProd TBLP ON   TBLP.idProd=TBLO.idProd " +
	                        "WHERE   TBLCY.sMayor=='dudley'";

	            sw.Add("$$tblCity$$.sMayor=='dudley'", Op.And);

	            sTest = sw.GetWhere(sBase);
	            Assert.AreEqual(sExpected, sTest);
	        }

			[Test]
    		[TestCase("12/25/2013", "'2013-12-25T00:00:00.000'")]
			[TestCase("12/25/2013 23:13:02", "'2013-12-25T23:13:02.000'")]
			[TestCase("12/25/2013 08:00:00Z", "'2013-12-25T00:00:00.000'")]
			[TestCase("12/25/2013 23:13:02.12345", "'2013-12-25T23:13:02.123'")]
			[TestCase(null, "null")]
    		public void NullableTests(string sDateTime, string sExpected)
			{
    			DateTime? dttm = sDateTime == null ? (DateTime?)null : (DateTime?)DateTime.Parse(sDateTime);
    			Assert.AreEqual(sExpected, Sql.Nullable(dttm));
			}

    		[Test]
    		[TestCase("foo", "foo")]
    		[TestCase("fo'o", "fo''o")]
    		public void SqlifyTests(string sInput, string sExpected)
			{
    			Assert.AreEqual(sExpected, Sql.Sqlify(sInput));
			}


	    }
	}

	public class Sql
	{
		SqlConnection m_sqlc;
		SqlTransaction m_sqlt;

		public Sql() { m_sqlc = null; m_sqlt = null;}
		public Sql(SqlConnection sqlc, SqlTransaction sqlt)
		{
			m_sqlc = sqlc;
			m_sqlt = sqlt;
		}

		public SqlConnection Connection { get { return m_sqlc;}}
		public SqlTransaction Transaction { get { return m_sqlt;}}


		/* O P E N  C O N N E C T I O N */
		/*----------------------------------------------------------------------------
			%%Function: OpenConnection
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public static SR OpenConnection(out Sql sql, string sResourceConnString)
		{
			//System.Threading.Thread.Sleep(150);
			sql = null;
			SqlConnection sqlc = new SqlConnection(sResourceConnString);

			try
				{
				sqlc.Open(); 
				}
			catch (Exception exc)
				{
				return SR.Failed(exc.ToString());
				}

			sql = new Sql(sqlc, null);
			return SR.Success();
		}

	    public static SR SrSetupStaticSql(ref Sql sql, string sResourceConnString, out bool fLocalSql)
	    {
	        SR sr;
	        fLocalSql = false;

	        try
	            {
	            if (sql == null)
	                {
	                sr = Sql.OpenConnection(out sql, sResourceConnString);
	                if (sr.Succeeded)
	                    fLocalSql = true;
	                }
	            sr = SR.Success();
	            }
	        catch (Exception exc)
	            {
	            sr = SR.Failed(exc.Message);
	            }

	        return sr;
	    }

	    public static SR SrReleaseStaticSql(SR sr, ref Sql sql, bool fLocalSql)
        {
            if (fLocalSql)
				sql.Close();

			return sr;

        }

		/* E X E C U T E  N O N  Q U E R Y */
		/*----------------------------------------------------------------------------
			%%Function: ExecuteNonQuery
			%%Qualified: BhSvc.Sql.ExecuteNonQuery
			%%Contact: rlittle

			Execute the given non query.  There is only a failed/success response
		----------------------------------------------------------------------------*/
		static public SR ExecuteNonQuery(Sql sql, string s, string sResourceConnString)
		{
			SR sr;
		    bool fLocalSql;

		    if (!(sr = SrSetupStaticSql(ref sql, sResourceConnString, out fLocalSql)).Succeeded)
		        return sr;

    		try
				{
				SqlCommand sqlcmd = sql.CreateCommand();

				sqlcmd.CommandText = s;
                sqlcmd.Transaction = sql.Transaction;
				sqlcmd.ExecuteNonQuery();

				sr = SR.Success();
				}
    		catch (Exception exc)
				{
    			sr = SR.Failed(exc.Message);
				}

		    return SrReleaseStaticSql(sr, ref sql, fLocalSql);
		}

	    public static int NExecuteScalar(Sql sql, string s, string sResourceConnString, int nDefaultValue)
	    {
	        SR sr;
	        bool fLocalSql;
	        int nRet;

	        if (!(sr = SrSetupStaticSql(ref sql, sResourceConnString, out fLocalSql)).Succeeded)
	            return nDefaultValue;

	        try
	            {
    	        nRet = sql.NExecuteScalar(s);
	            }
	        catch
	            {
                nRet = nDefaultValue;
	            }


	        if (!SrReleaseStaticSql(sr, ref sql, fLocalSql).Succeeded)
	            return nDefaultValue;

	        return nRet;
	    }

	    /* E X E C U T E  N O N  Q U E R Y */
		/*----------------------------------------------------------------------------
			%%Function: ExecuteNonQuery
			%%Qualified: TCore.Sql.ExecuteNonQuery
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		static public SR ExecuteNonQuery(string s, string sResourceConnString)
		{
    		return ExecuteNonQuery(null, s, sResourceConnString);
		}

		/* N  E X E C U T E  S C A L A R */
		/*----------------------------------------------------------------------------
			%%Function: NExecuteScalar
			%%Qualified: BhSvc.Sql.NExecuteScalar
			%%Contact: rlittle

			Execute the scalar command, returning the result.  
		----------------------------------------------------------------------------*/
		public int NExecuteScalar(string sQuery)
		{
			SqlCommand sqlcmd = this.Connection.CreateCommand();
			sqlcmd.CommandText = sQuery;
			sqlcmd.Transaction = this.Transaction;

			return(int)sqlcmd.ExecuteScalar();
		}


        /* S  E X E C U T E  S C A L A R */
        /*----------------------------------------------------------------------------
            %%Function: SExecuteScalar
            %%Qualified: BhSvc.Sql.SExecuteScalar
            %%Contact: rlittle

            Execute the scalar command, returning the result.  
        ----------------------------------------------------------------------------*/
        public string SExecuteScalar(string sQuery)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (string)sqlcmd.ExecuteScalar();
        }

        public DateTime DttmExecuteScalar(string sQuery)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (DateTime)sqlcmd.ExecuteScalar();
        }

		public bool InTransaction { get { return m_sqlt != null;}}

		/* B E G I N  T R A N S A C T I O N */
		/*----------------------------------------------------------------------------
			%%Function: BeginTransaction
			%%Qualified: BhSvc.Sql.BeginTransaction
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public SR BeginTransaction()
		{
			if (m_sqlt != null)
				return SR.Failed("Already in a transaction!");

			m_sqlt = m_sqlc.BeginTransaction();
			return SR.Success();
		}

		public SR Rollback()
		{
			if (m_sqlt == null)
				return SR.Failed("trying to rollback outside of a transaction");

			m_sqlt.Rollback();
			m_sqlt = null;
			return SR.Success();
		}

		public SR Commit()
		{
			if (m_sqlt == null)
				return SR.Failed("trying to commit outside of a transaction");

			m_sqlt.Commit();
			m_sqlt = null;
			return SR.Success();
		}

		public SR Close()
		{
			if (m_sqlt != null)
				return SR.Failed("trying to close with a pending transaction!");

			m_sqlc.Close();
			return SR.Success();
		}

		public SqlCommand CreateCommand()
		{
			return m_sqlc.CreateCommand();
		}

		public SR ExecuteReader(string sQuery, out SqlReader sqlr, string sResourceConnString)
		{
			sqlr = new SqlReader(this);

            return sqlr.ExecuteQuery(sQuery, sResourceConnString);
		}

		/* E X E C U T E  Q U E R Y */
		/*----------------------------------------------------------------------------
			%%Function: ExecuteQuery
			%%Qualified: RwpSvc.Sql.ExecuteQuery
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public static SR ExecuteQuery(string sQuery, IQueryResult iqr, string sResourceConnString)
		{
    		Sql sql;

    		SR sr = Sql.OpenConnection(out sql, sResourceConnString);

    		if (!sr.Result)
    			return sr;

    		return ExecuteQuery(sql, sQuery, iqr, sResourceConnString);
		}

		/* E X E C U T E  Q U E R Y */
		/*----------------------------------------------------------------------------
			%%Function: ExecuteQuery
			%%Qualified: BhSvc.Sql.ExecuteQuery
			%%Contact: rlittle

			Execute the given query and send the results to IQueryResult...
		----------------------------------------------------------------------------*/
		public static SR ExecuteQuery(Sql sql, string sQuery, IQueryResult iqr, string sResourceConnString)
		{
			SR sr;
			SqlReader sqlr;
			int iRecordSet = 0;

			sqlr = new SqlReader(sql);

            sr = sqlr.ExecuteQuery(sQuery, sResourceConnString);
			if (!sr.Succeeded)
				return sr;
			
			do
				{
				while (sqlr.Reader.Read())
					{
					if (!iqr.FAddResultRow(sqlr, iRecordSet))
						return SR.Failed("FAddResultRow failed!");
					}
				iRecordSet++;
				} while (sqlr.Reader.NextResult());

			sqlr.Close();
			return sqlr.Result;
		}


		/* S Q L I F Y */
		/*----------------------------------------------------------------------------
			%%Function: Sqlify
			%%Qualified: RwpSvc.Sql.Sqlify
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public static string Sqlify(string s)
		{
			return s.Replace("'", "''");
		}

		public static string Nullable(string s)
		{
			if (s == null)
				return "null";
			else
				return String.Format("'{0}'", s);
		}

		public static string Nullable(int? n)
		{
			if (n == null)
				return "null";
			else
				return String.Format("{0}", n.Value);
		}

    	public static string Nullable(bool? f)
		{
    		if (f == null)
    			return "null";
    		else
    			return f.Value ? "1" : "0";
		}

		/* N U L L A B L E */
		/*----------------------------------------------------------------------------
			%%Function: Nullable
			%%Qualified: tw.twsvc:SqlReader.Nullable
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public static string Nullable(DateTime? dttm)
		{
			if (dttm == null)
				return "null";
			else
				return String.Format("'{0:D4}-{1:D2}-{2:D2}T{3:D2}:{4:D2}:{5:D2}.{6:D3}'", 
									 dttm.Value.Year, dttm.Value.Month, dttm.Value.Day, dttm.Value.Hour, 
									 dttm.Value.Minute, dttm.Value.Second, dttm.Value.Millisecond);
		}

		public static string Nullable(Guid? guid)
		{
			if (guid == null)
				return "null";
			else
				return String.Format("'{0}'", guid.Value.ToString());
		}

	}

	public class SqlReader
	{
		Sql m_sql;
		SqlDataReader m_sqlr;
		bool m_fAttached;

		SR m_sr;

		public SqlReader()
		{
			m_fAttached = false;
		}

		public SqlReader(Sql sql)
		{
			Attach(sql);
		}

		/* A T T A C H */
		/*----------------------------------------------------------------------------
			%%Function: Attach
			%%Qualified: tw.twsvc:SqlReader.Attach
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void Attach(Sql sql)
		{
			m_sql = sql;
			if (m_sql != null)
				m_fAttached = true;
		}

	    public bool FExecuteQuery(string sQuery, string sResourceConnString)
	    {
	        SR sr = ExecuteQuery(sQuery, sResourceConnString);

	        return sr.Succeeded;
	    }

	    /* F  E X E C U T E  Q U E R Y */
		/*----------------------------------------------------------------------------
			%%Function: FExecuteQuery
			%%Qualified: BhSvc.SqlReader.FExecuteQuery
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public SR ExecuteQuery(string sQuery, string sResourceConnString)
		{
			if (m_sql == null)
				{
				m_sr = Sql.OpenConnection(out m_sql, sResourceConnString);
				m_fAttached = false;
				}

			if ((m_sr != null && !m_sr.Succeeded) || m_sql == null)
				return m_sr;

			SqlCommand sqlcmd = m_sql.Connection.CreateCommand();
			sqlcmd.CommandText = sQuery;
			sqlcmd.Transaction = m_sql.Transaction;

			m_sr = null;
			if (m_sqlr != null)
				m_sqlr.Close();

			try
				{
				m_sqlr = sqlcmd.ExecuteReader();
				}
			catch (Exception exc)
				{
				throw new Exception(String.Format("ExcuteReader failed on query: {0}; {1}: {2}", sQuery, exc.Message, exc.StackTrace));
                }
			return SR.Success();
		}

		/* C L O S E */
		/*----------------------------------------------------------------------------
			%%Function: Close
			%%Qualified: BhSvc.SqlReader.Close
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void Close()
		{
			if (m_sqlr != null)
				m_sqlr.Close();

			if (!m_fAttached)
				{
				m_sql.Close();
				m_sql = null;
				}
		}

		public SqlDataReader Reader { get { return m_sqlr;}}
		public bool Succeeded { get { return m_sr == null ? true : m_sr.Succeeded;}}
		public string Failed { get { return m_sr == null ? "" : m_sr.Reason;}}
		public SR Result { get { return m_sr == null ? SR.Success() : m_sr;}}
	}

	// ================================================================================
	// X M L  Q U E R Y  R E S U L T
	// ================================================================================

	// implementation of IQueryResult for XML results
	class XmlQueryResult : IQueryResult
		{
		StringBuilder m_sb;

		/* F  A D D  R E S U L T  R O W */
		/*----------------------------------------------------------------------------
			%%Function: StatsSvc.StatsQuery.FAddResultRow
			%%Contact: rlittle

			Implemenation if IQueryResult.FAddResultRow for XML results (adds the
			current result to a string containing XML)

			This only handles 1 recordset
		----------------------------------------------------------------------------*/
		public bool FAddResultRow(SqlReader sqlr, int iRecordSet)
		{
			if (iRecordSet != 0)
				throw new Exception("internal error - XmlQueryResult should never get multiple recordsets!");

			m_sb.Append("<line>");
			for (int i = 0, iMac = sqlr.Reader.FieldCount; i < iMac; i++)
				{
				m_sb.Append("<");
				m_sb.Append(Regex.Replace(sqlr.Reader.GetName(i), " ", "_"));
				m_sb.Append(">");
				Object o = sqlr.Reader.GetSqlValue(i);
				m_sb.Append(o.ToString());
				m_sb.Append("</");
				m_sb.Append(Regex.Replace(sqlr.Reader.GetName(i), " ", "_"));
				m_sb.Append(">");
				}
			m_sb.Append("</line>");
			return true;
		}

		/* X M L  Q U E R Y  R E S U L T */
		/*----------------------------------------------------------------------------
			%%Function: StatsSvc.StatsQuery.XmlQueryResult
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public XmlQueryResult()
		{
			m_sb = new StringBuilder(2048);
		}

		/* B E G I N  R E S U L T  S E T */
		/*----------------------------------------------------------------------------
			%%Function: StatsSvc.StatsQuery.BeginResultSet
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void BeginResultSet()
		{
			m_sb.Append("<results>");
		}

		/* E N D  R E S U L T  S E T */
		/*----------------------------------------------------------------------------
			%%Function: StatsSvc.StatsQuery.EndResultSet
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
		public void EndResultSet()
		{
			m_sb.Append("</results>");
		}

		public string XML { get { return m_sb.ToString();}}
	}
}
