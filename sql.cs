using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using TCore.Exceptions;

namespace TCore
{
    public delegate void CustomizeCommandDelegate(SqlCommand command);

    // ===============================================================================
    //  I  Q U E R Y  R E S U L T 
    // ===============================================================================
    public interface IQueryResult
    {
        bool FAddResultRow(SqlReader sqlr, int iRecordSet);
    }

    public class Sql
    {
        SqlConnection m_sqlc;
        SqlTransaction m_sqlt;

        public Sql() { m_sqlc = null; m_sqlt = null; }
        public Sql(SqlConnection sqlc, SqlTransaction sqlt)
        {
            m_sqlc = sqlc;
            m_sqlt = sqlt;
        }

        public SqlConnection Connection { get { return m_sqlc; } }
        public SqlTransaction Transaction { get { return m_sqlt; } }


        /*----------------------------------------------------------------------------
            %%Function: OpenConnection
            %%Qualified: TCore.Sql.OpenConnection
        ----------------------------------------------------------------------------*/
        public static Sql OpenConnection(string sResourceConnString)
        {
            SqlConnection sqlc = new SqlConnection(sResourceConnString);

            sqlc.Open();

            return new Sql(sqlc, null);
        }

        /*----------------------------------------------------------------------------
            %%Function: SrSetupStaticSql
            %%Qualified: TCore.Sql.SrSetupStaticSql
        ----------------------------------------------------------------------------*/
        public static Sql SetupStaticSql(Sql sqlIn, string sResourceConnString, out bool fLocalSql)
        {
            fLocalSql = false;

            if (sqlIn == null)
            {
                sqlIn = OpenConnection(sResourceConnString);
                fLocalSql = true;
            }

            return sqlIn;
        }

        /*----------------------------------------------------------------------------
            %%Function: SrReleaseStaticSql
            %%Qualified: TCore.Sql.SrReleaseStaticSql
        ----------------------------------------------------------------------------*/
        public static void ReleaseStaticSql(ref Sql sql, bool fLocalSql)
        {
            if (fLocalSql)
                sql.Close();
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery
        ----------------------------------------------------------------------------*/
        public static void ExecuteNonQuery(
            Sql sql,
            SqlCommandTextInit cmdText,
            string sResourceConnString,
            CustomizeCommandDelegate customizeParams = null)
        {
            ExecuteNonQuery(sql, cmdText.CommandText, sResourceConnString, customizeParams, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery

			Execute the given non query.  There is only a failed/success response
        ----------------------------------------------------------------------------*/
        public static void ExecuteNonQuery(
            Sql sql,
            string s,
            string sResourceConnString,
            CustomizeCommandDelegate customizeParams = null,
            Dictionary<string, string> aliases = null)
        {
            sql = SetupStaticSql(sql, sResourceConnString, out bool fLocalSql);

            SqlCommand sqlcmd = sql.CreateCommand();

            if (aliases != null)
                s = SqlWhere.ExpandAliases(s, aliases);

            sqlcmd.CommandText = s;
            if (customizeParams != null)
                customizeParams(sqlcmd);

            sqlcmd.Transaction = sql.Transaction;
            sqlcmd.ExecuteNonQuery();


            ReleaseStaticSql(ref sql, fLocalSql);
        }

        public void ExecuteNonQuery(
            SqlCommandTextInit cmdText,
            CustomizeCommandDelegate customizeParams = null)
        {
            ExecuteNonQuery(cmdText.CommandText, customizeParams, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery
        ----------------------------------------------------------------------------*/
        public void ExecuteNonQuery(
            string s,
            CustomizeCommandDelegate customizeParams = null,
            Dictionary<string, string> aliases = null)
        {
            SqlCommand sqlcmd = CreateCommand();

            if (aliases != null)
                s = SqlWhere.ExpandAliases(s, aliases);

            sqlcmd.CommandText = s;
            if (customizeParams != null)
                customizeParams(sqlcmd);

            sqlcmd.Transaction = Transaction;
            sqlcmd.ExecuteNonQuery();
        }

        public static int NExecuteScalar(
            Sql sql,
            SqlCommandTextInit cmdText,
            string sResourceConnString,
            int nDefaultValue)
        {
            return NExecuteScalar(sql, cmdText.CommandText, sResourceConnString, nDefaultValue, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: NExecuteScalar
            %%Qualified: TCore.Sql.NExecuteScalar
        ----------------------------------------------------------------------------*/
        public static int NExecuteScalar(
            Sql sql,
            string s,
            string sResourceConnString,
            int nDefaultValue,
            Dictionary<string, string> aliases = null)
        {
            try
            {
                sql = SetupStaticSql(sql, sResourceConnString, out bool fLocalSql);
                int nRet = sql.NExecuteScalar(s, aliases);
                ReleaseStaticSql(ref sql, fLocalSql);

                return nRet;
            }
            catch
            {
                return nDefaultValue;
            }
        }

        public static void ExecuteNonQuery(
            SqlCommandTextInit cmdText,
            string sResourceConnString)
        {
            ExecuteNonQuery(null, cmdText.CommandText, sResourceConnString, null, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery
        ----------------------------------------------------------------------------*/
        public static void ExecuteNonQuery(
            string s,
            string sResourceConnString,
            Dictionary<string, string> aliases = null)
        {
            ExecuteNonQuery(null, s, sResourceConnString, null, aliases);
        }

        public int NExecuteScalar(SqlCommandTextInit cmdText)
        {
            return NExecuteScalar(cmdText.CommandText, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: NExecuteScalar
            %%Qualified: TCore.Sql.NExecuteScalar

   			Execute the scalar command, returning the result.  
        ----------------------------------------------------------------------------*/
        public int NExecuteScalar(string sQuery, Dictionary<string, string> aliases = null)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            if (aliases != null)
                sQuery = SqlWhere.ExpandAliases(sQuery, aliases);
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (int)sqlcmd.ExecuteScalar();
        }

        public string SExecuteScalar(SqlCommandTextInit cmdText)
        {
            return SExecuteScalar(cmdText.CommandText, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: SExecuteScalar
            %%Qualified: TCore.Sql.SExecuteScalar

            Execute the scalar command, returning the result.  
        ----------------------------------------------------------------------------*/
        public string SExecuteScalar(string sQuery, Dictionary<string, string> aliases = null)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            if (aliases != null)
                sQuery = SqlWhere.ExpandAliases(sQuery, aliases);
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (string)sqlcmd.ExecuteScalar();
        }

        public DateTime DttmExecuteScalar(SqlCommandTextInit cmdText)
        {
            return DttmExecuteScalar(cmdText.CommandText, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: DttmExecuteScalar
            %%Qualified: TCore.Sql.DttmExecuteScalar
        ----------------------------------------------------------------------------*/
        public DateTime DttmExecuteScalar(string sQuery, Dictionary<string, string> aliases = null)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            if (aliases != null)
                sQuery = SqlWhere.ExpandAliases(sQuery, aliases);
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (DateTime)sqlcmd.ExecuteScalar();
        }

        public bool InTransaction { get { return m_sqlt != null; } }

        /* B E G I N  T R A N S A C T I O N */
        /*----------------------------------------------------------------------------
			%%Function: BeginTransaction
			%%Qualified: BhSvc.Sql.BeginTransaction
			%%Contact: rlittle

		----------------------------------------------------------------------------*/
        public void BeginTransaction()
        {
            if (InTransaction)
                throw new TcSqlExceptionInTransaction("cannot nest transactions");

            m_sqlt = m_sqlc.BeginTransaction();
        }

        /*----------------------------------------------------------------------------
            %%Function: Rollback
            %%Qualified: TCore.Sql.Rollback
        ----------------------------------------------------------------------------*/
        public void Rollback()
        {
            if (!InTransaction)
                throw new TcSqlExceptionNotInTransaction("can't rollback if not in transaction");

            m_sqlt.Rollback();
            m_sqlt = null;
        }

        /*----------------------------------------------------------------------------
            %%Function: Commit
            %%Qualified: TCore.Sql.Commit
        ----------------------------------------------------------------------------*/
        public void Commit()
        {
            if (!InTransaction)
                throw new TcSqlExceptionNotInTransaction("can't commit if not in transaction");

            m_sqlt.Commit();
            m_sqlt = null;
        }

        /*----------------------------------------------------------------------------
            %%Function: Close
            %%Qualified: TCore.Sql.Close
        ----------------------------------------------------------------------------*/
        public void Close()
        {
            if (InTransaction)
                throw new TcSqlExceptionNotInTransaction("can't close with pending transaction");

            m_sqlc.Close();
            m_sqlc.Dispose();
        }

        /*----------------------------------------------------------------------------
            %%Function: CreateCommand
            %%Qualified: TCore.Sql.CreateCommand
        ----------------------------------------------------------------------------*/
        public SqlCommand CreateCommand()
        {
            return m_sqlc.CreateCommand();
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteReader
            %%Qualified: TCore.Sql.ExecuteReader
        ----------------------------------------------------------------------------*/
        public void ExecuteReader(
            string sQuery,
            out SqlReader sqlr,
            string sResourceConnString,
            Dictionary<string, string> aliases = null)
        {
            sqlr = new SqlReader(this);

            sqlr.ExecuteQuery(sQuery, sResourceConnString, null, aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.Sql.ExecuteQuery
        ----------------------------------------------------------------------------*/
        public static void ExecuteQuery(
            string sQuery,
            IQueryResult iqr,
            string sResourceConnString, 
            Dictionary<string, string> aliases = null)
        {
            Sql sql = Sql.OpenConnection(sResourceConnString);

            ExecuteQuery(sql, sQuery, iqr, sResourceConnString, aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.Sql.ExecuteQuery

			Execute the given query and send the results to IQueryResult...
        ----------------------------------------------------------------------------*/
        public static void ExecuteQuery(
            Sql sql,
            string sQuery,
            IQueryResult iqr,
            string sResourceConnString,
            Dictionary<string, string> aliases = null,
            CustomizeCommandDelegate customizeDelegate = null)
        {
            SqlReader sqlr;
            int iRecordSet = 0;

            sqlr = new SqlReader(sql);

            sqlr.ExecuteQuery(sQuery, sResourceConnString, customizeDelegate, aliases);

            do
            {
                while (sqlr.Reader.Read())
                {
                    if (!iqr.FAddResultRow(sqlr, iRecordSet))
                        throw new TcSqlException("FAddResultRow failed!");
                }
                iRecordSet++;
            } while (sqlr.Reader.NextResult());

            sqlr.Close();
        }


        /*----------------------------------------------------------------------------
            %%Function: Sqlify
            %%Qualified: TCore.Sql.Sqlify

            consider using command parameters
        ----------------------------------------------------------------------------*/
        public static string Sqlify(string s)
        {
            return s.Replace("'", "''");
        }

        /*----------------------------------------------------------------------------
            %%Function: Nullable
            %%Qualified: TCore.Sql.Nullable
        ----------------------------------------------------------------------------*/
        public static string Nullable(string s)
        {
            if (s == null)
                return "null";
            else
                return String.Format("'{0}'", s);
        }

        /*----------------------------------------------------------------------------
            %%Function: Nullable
            %%Qualified: TCore.Sql.Nullable
        ----------------------------------------------------------------------------*/
        public static string Nullable(int? n)
        {
            if (n == null)
                return "null";
            else
                return String.Format("{0}", n.Value);
        }

        /*----------------------------------------------------------------------------
            %%Function: Nullable
            %%Qualified: TCore.Sql.Nullable
        ----------------------------------------------------------------------------*/
        public static string Nullable(bool? f)
        {
            if (f == null)
                return "null";
            else
                return f.Value ? "1" : "0";
        }

        /*----------------------------------------------------------------------------
            %%Function: Nullable
            %%Qualified: TCore.Sql.Nullable
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

        /*----------------------------------------------------------------------------
            %%Function: Nullable
            %%Qualified: TCore.Sql.Nullable
        ----------------------------------------------------------------------------*/
        public static string Nullable(Guid? guid)
        {
            if (guid == null)
                return "null";
            else
                return String.Format("'{0}'", guid.Value.ToString());
        }

    }

}
