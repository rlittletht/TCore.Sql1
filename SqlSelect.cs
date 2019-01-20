using System;
using System.Collections.Generic;
using System.Text;

namespace TCore
{
    public class SqlSelect
    {
        List<SqlInnerJoin> m_plij = new List<SqlInnerJoin>();
        private string m_sBase;
        private SqlWhere m_sw;
        private string m_sOrderBy;

        public override string ToString()
        {
            string sBase = m_sBase;

            if (sBase == null)
                sBase = "";

            StringBuilder sb = new StringBuilder(256);

            sb.Append(m_sw.ExpandAliases(sBase));

            if (m_plij != null)
            {
                foreach (SqlInnerJoin ij in m_plij)
                {
                    sb.Append(" ");
                    sb.Append(m_sw.ExpandAliases(ij.ToString()));
                }
            }

            string sBaseForWhere = sb.ToString();

            return String.Format("{0} {1}", m_sw.GetWhere(sBaseForWhere),
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

        public void AddInnerJoin(SqlInnerJoin ij)
        {
            m_plij.Add(ij);
        }

    }
}