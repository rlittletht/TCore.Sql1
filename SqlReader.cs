using System;
using System.Collections.Generic;
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
            CustomizeCommandDelegate customizeDelegate = null)
        {
            ExecuteQuery(cmdText.CommandText, sResourceConnString, customizeDelegate, cmdText.Aliases);
        }

        /*----------------------------------------------------------------------------
            %%Function: ExecuteQuery
            %%Qualified: TCore.SqlReader.ExecuteQuery
        ----------------------------------------------------------------------------*/
        public void ExecuteQuery(
            string sQuery,
            string sResourceConnString,
            CustomizeCommandDelegate customizeDelegate = null, 
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

            if (customizeDelegate != null)
                customizeDelegate(sqlcmd);

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
            TCore.CustomizeCommandDelegate customizeDelegate = null) where T: new()
        {
            SqlReader sqlr = null;

            if (delegateReader == null)
                throw new Exception("must provide delegate reader");

            try
            {
                string sCmd = sQuery;

                sqlr = new(sql);
                sqlr.ExecuteQuery(sQuery, null, customizeDelegate);

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

        public delegate void DelegateMultiSetReader<T>(SqlReader sqlr, Guid crids, int recordSet, ref T t);

        /// <summary>
        /// Execute the given query. This supports queries that return multiple recordsets.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">already connected SQL object</param>
        /// <param name="crids">short correlation id (guid)</param>
        /// <param name="sQuery">t-sql query (multiple recordsets ok)</param>
        /// <param name="delegateReader">delegate that will be called for every record</param>
        /// <param name="customizeDelegate">optional customization delegate (for adding parameter values)</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        /// <exception cref="TcSqlExceptionNoResults"></exception>
        /*----------------------------------------------------------------------------
            %%Function: DoGenericQueryDelegateRead
            %%Qualified: TCore.SqlReader.DoGenericQueryDelegateRead<T>
        ----------------------------------------------------------------------------*/
        public static T DoGenericMultiSetQueryDelegateRead<T>(
            Sql sql,
            Guid crids,
            string sQuery,
            DelegateMultiSetReader<T> delegateReader,
            TCore.CustomizeCommandDelegate customizeDelegate = null) where T : new()
        {
            SqlReader sqlr = null;

            if (delegateReader == null)
                throw new Exception("must provide delegate reader");

            try
            {
                string sCmd = sQuery;

                sqlr = new(sql);
                sqlr.ExecuteQuery(sQuery, null, customizeDelegate);

                int recordSet = 0;

                T t = new();
                do
                {
                    bool fOnce = false;

                    while (sqlr.Reader.Read())
                    {
                        delegateReader(sqlr, crids, recordSet, ref t);
                        fOnce = true;
                    }

                    if (!fOnce)
                        throw new TcSqlExceptionNoResults();

                    recordSet++;
                } while (sqlr.Reader.NextResult());

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
            {
                m_sqlr.Close();
                m_sqlr.Dispose();
            }

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