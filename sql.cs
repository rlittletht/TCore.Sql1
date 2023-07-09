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
//using TCore.Logging;

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
        public static SR ExecuteNonQuery(
            Sql sql,
            string s,
            string sResourceConnString,
            CustomizeCommandDel customizeParams = null)
        {
            SR sr;

            if (!(sr = SrSetupStaticSql(ref sql, sResourceConnString, out bool fLocalSql)).Succeeded)
                return sr;

            try
            {
                SqlCommand sqlcmd = sql.CreateCommand();

                sqlcmd.CommandText = s;
                if (customizeParams != null)
                    customizeParams(sqlcmd);

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

        public SR ExecuteNonQuery(string s, CustomizeCommandDel customizeParams = null)
        {
            SR sr;

            try
            {
                SqlCommand sqlcmd = CreateCommand();

                sqlcmd.CommandText = s;
                if (customizeParams != null)
                    customizeParams(sqlcmd);

                sqlcmd.Transaction = Transaction;
                sqlcmd.ExecuteNonQuery();

                sr = SR.Success();
            }
            catch (Exception exc)
            {
                sr = SR.Failed(exc.Message);
            }

            return sr;
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

            return (int)sqlcmd.ExecuteScalar();
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

        public bool InTransaction { get { return m_sqlt != null; } }

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

}
