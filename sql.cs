using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using TCore.Exceptions;

namespace TCore
{
    public delegate void CustomizeCommandDel(SqlCommand command);

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

			Execute the given non query.  There is only a failed/success response
        ----------------------------------------------------------------------------*/
        public static void ExecuteNonQuery(
            Sql sql,
            string s,
            string sResourceConnString,
            CustomizeCommandDel customizeParams = null)
        {
            sql = SetupStaticSql(sql, sResourceConnString, out bool fLocalSql);

            SqlCommand sqlcmd = sql.CreateCommand();

            sqlcmd.CommandText = s;
            if (customizeParams != null)
                customizeParams(sqlcmd);

            sqlcmd.Transaction = sql.Transaction;
            sqlcmd.ExecuteNonQuery();


            ReleaseStaticSql(ref sql, fLocalSql);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery
        ----------------------------------------------------------------------------*/
        public void ExecuteNonQuery(string s, CustomizeCommandDel customizeParams = null)
        {
            SqlCommand sqlcmd = CreateCommand();

            sqlcmd.CommandText = s;
            if (customizeParams != null)
                customizeParams(sqlcmd);

            sqlcmd.Transaction = Transaction;
            sqlcmd.ExecuteNonQuery();
        }

        /*----------------------------------------------------------------------------
            %%Function: NExecuteScalar
            %%Qualified: TCore.Sql.NExecuteScalar
        ----------------------------------------------------------------------------*/
        public static int NExecuteScalar(Sql sql, string s, string sResourceConnString, int nDefaultValue)
        {
            try
            {
                sql = SetupStaticSql(sql, sResourceConnString, out bool fLocalSql);
                int nRet = sql.NExecuteScalar(s);
                ReleaseStaticSql(ref sql, fLocalSql);

                return nRet;
            }
            catch
            {
                return nDefaultValue;
            }
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteNonQuery
            %%Qualified: TCore.Sql.ExecuteNonQuery
        ----------------------------------------------------------------------------*/
        public static void ExecuteNonQuery(string s, string sResourceConnString)
        {
            ExecuteNonQuery(null, s, sResourceConnString);
        }

        /*----------------------------------------------------------------------------
            %%Function: NExecuteScalar
            %%Qualified: TCore.Sql.NExecuteScalar

   			Execute the scalar command, returning the result.  
        ----------------------------------------------------------------------------*/
        public int NExecuteScalar(string sQuery)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (int)sqlcmd.ExecuteScalar();
        }

        /*----------------------------------------------------------------------------
            %%Function: SExecuteScalar
            %%Qualified: TCore.Sql.SExecuteScalar

            Execute the scalar command, returning the result.  
        ----------------------------------------------------------------------------*/
        public string SExecuteScalar(string sQuery)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = this.Transaction;

            return (string)sqlcmd.ExecuteScalar();
        }

        /*----------------------------------------------------------------------------
            %%Function: DttmExecuteScalar
            %%Qualified: TCore.Sql.DttmExecuteScalar
        ----------------------------------------------------------------------------*/
        public DateTime DttmExecuteScalar(string sQuery)
        {
            SqlCommand sqlcmd = this.Connection.CreateCommand();
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
        public void ExecuteReader(string sQuery, out SqlReader sqlr, string sResourceConnString)
        {
            sqlr = new SqlReader(this);

            sqlr.ExecuteQuery(sQuery, sResourceConnString);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.Sql.ExecuteQuery
        ----------------------------------------------------------------------------*/
        public static void ExecuteQuery(string sQuery, IQueryResult iqr, string sResourceConnString)
        {
            Sql sql = Sql.OpenConnection(sResourceConnString);

            ExecuteQuery(sql, sQuery, iqr, sResourceConnString);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.Sql.ExecuteQuery

			Execute the given query and send the results to IQueryResult...
        ----------------------------------------------------------------------------*/
        public static void ExecuteQuery(Sql sql, string sQuery, IQueryResult iqr, string sResourceConnString)
        {
            SqlReader sqlr;
            int iRecordSet = 0;

            sqlr = new SqlReader(sql);

            sqlr.ExecuteQuery(sQuery, sResourceConnString);

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
