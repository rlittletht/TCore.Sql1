using System;
using System.Collections.Generic;

namespace TCore
{
    public class SqlSelect
    {
        private string m_sBase;
        private SqlWhere m_sw;
        private string m_sOrderBy;

        public override string ToString()
        {
            return String.Format("{0} {1}", m_sw.GetWhere(m_sBase),
                m_sOrderBy == null ? "" : m_sw.ExpandAliases(m_sOrderBy));
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

        public SqlWhere Where
        {
            get { return m_sw; }
        }
    }
}