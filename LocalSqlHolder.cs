using System;
using TCore.Exceptions;

namespace TCore
{
    // The idea here is that you can create one of these on your stack and when it passes out of
    // scope, the local SQL will get closed for you. To make this work, you MUST use "using"
    //
    // e.g.

    public class LocalSqlHolder
#if USINGPARADIGM
        : IDisposable
#endif
    {
        private Sql m_sql;
        private bool m_fLocal;
        private Guid m_crids;

        public Sql Sql => m_sql;
        public Guid Crids => m_crids;
#if LoggingIntegrated
        public CorrelationID Crid => CorrelationID.FromCrids(m_crids);
#endif

        public static implicit operator Sql(LocalSqlHolder lsh)
        {
            return lsh.Sql;
        }

        public LocalSqlHolder(Sql sql, Guid crids, string sConnectionString)
        {
            m_crids = crids;
            m_sql = sql;
            SR sr = Sql.SrSetupStaticSql(ref m_sql, sConnectionString, out m_fLocal);

            if (!sr.Succeeded)
                throw new TcException(sr.Reason, m_crids);
        }

        public void Close()
        {
            Sql.SrReleaseStaticSql(SR.Success(), ref m_sql, m_fLocal);
        }

#if USINGPARADIGM
        public void Dispose()
        {
            Dispose(true);
        }

        ~LocalSqlHolder()
        {
            Dispose(false);
        }

        protected void Dispose(bool fDisposing)
        {
            if (fDisposing) // safe to reference managed resources here
                {
                Close();
                }            
        }
#endif
    }

}