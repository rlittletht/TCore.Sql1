using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.Data.SqlClient;
using TCore;

namespace TCore
{
    public class SqlReader
    {
        Sql m_sql;
        SqlDataReader m_sqlr;
        bool m_fAttached;
        private Guid m_crids;
        private bool m_fSucceeded = false;

        public SqlReader()
        {
            m_fAttached = false;
            m_crids = Guid.Empty;
        }

        public SqlReader(Guid crids)
        {
            m_fAttached = false;
            m_crids = crids;
        }

        public SqlReader(Sql sql)
        {
            Attach(sql);
            m_crids = Guid.Empty;
        }

        public SqlReader(Sql sql, Guid crids)
        {
            Attach(sql);
            m_crids = crids;
        }

        /*----------------------------------------------------------------------------
            %%Function: Attach
            %%Qualified: TCore.SqlReader.Attach
        ----------------------------------------------------------------------------*/
        public void Attach(Sql sql)
        {
            m_sql = sql;
            if (m_sql != null)
                m_fAttached = true;
        }

        public void ExecuteQuery(
            SqlCommandTextInit cmdText,
            string sResourceConnString,
            CustomizeCommandDel customizeDel = null)
        {
            ExecuteQuery(cmdText.CommandText, sResourceConnString, customizeDel, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.SqlReader.ExecuteQuery
        ----------------------------------------------------------------------------*/
        public void ExecuteQuery(
            string sQuery,
            string sResourceConnString,
            CustomizeCommandDel customizeDel = null, 
            Dictionary<string, string> aliases = null)
        {
            if (m_sql == null)
            {
                m_sql = Sql.OpenConnection(sResourceConnString);
                m_fAttached = false;
            }

            if (m_sql == null)
                throw new TcSqlException("could not open sql connection");

            SqlCommand sqlcmd = m_sql.Connection.CreateCommand();
            sqlcmd.CommandText = sQuery;
            sqlcmd.Transaction = m_sql.Transaction;

            if (customizeDel != null)
                customizeDel(sqlcmd);

            if (m_sqlr != null)
                m_sqlr.Close();

            try
            {
                m_sqlr = sqlcmd.ExecuteReader();
            }
            catch (Exception exc)
            {
                throw new TcSqlException(m_crids, exc, "caught exception executing reader");
            }
        }

        public delegate void DelegateReader<T>(SqlReader sqlr, Guid crids, ref T t);

        /*----------------------------------------------------------------------------
            %%Function: DoGenericQueryDelegateRead
            %%Qualified: TCore.SqlReader.DoGenericQueryDelegateRead<T>
        ----------------------------------------------------------------------------*/
        public static T DoGenericQueryDelegateRead<T>(
            Sql sql,
            Guid crids,
            string sQuery,
            DelegateReader<T> delegateReader,
            TCore.CustomizeCommandDel customizeDel = null) where T: new()
        {
            SqlReader sqlr = null;

            if (delegateReader == null)
                throw new ArgumentNullException("must provide delegate reader");

            try
            {
                string sCmd = sQuery;

                sqlr = new(sql);
                sqlr.ExecuteQuery(sQuery, null, customizeDel);

                T t = new();
                bool fOnce = false;

                while (sqlr.Reader.Read())
                {
                    delegateReader(sqlr, crids, ref t);
                    fOnce = true;
                }

                if (!fOnce)
                    throw new TcSqlExceptionNoResults();

                return t;
            }
            finally
            {
                sqlr?.Close();
            }
        }

        /*----------------------------------------------------------------------------
            %%Function: Close
            %%Qualified: TCore.SqlReader.Close
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

        public SqlDataReader Reader => m_sqlr;
        public bool Succeeded => m_fSucceeded;
    }

}