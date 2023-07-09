using System;
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

        SR m_sr;

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

        /* F  E X E C U T E  Q U E R Y */
        /*----------------------------------------------------------------------------
            %%Function: FExecuteQuery
            %%Qualified: TCore.SqlReader.FExecuteQuery
            %%Contact: rlittle
    
        ----------------------------------------------------------------------------*/
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
                m_sr.CorrelationID = m_crids;
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
                return SR.FailedCorrelate(
                    String.Format("ExcuteReader failed on query: {0}; {1}: {2}", sQuery, exc.Message, exc.StackTrace),
                    m_crids);
            }

            return SR.SuccessCorrelate(m_crids);
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

        public SqlDataReader Reader
        {
            get { return m_sqlr; }
        }

        public bool Succeeded
        {
            get { return m_sr == null ? true : m_sr.Succeeded; }
        }

        public string Failed
        {
            get { return m_sr == null ? "" : m_sr.Reason; }
        }

        public SR Result
        {
            get { return m_sr == null ? SR.Success() : m_sr; }
        }
    }

}