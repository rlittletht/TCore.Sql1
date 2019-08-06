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
        private string m_sGroupBy;
        private string m_sAs;

        public override string ToString()
        {
            string sBase = m_sBase;

            if (sBase == null)
                sBase = "";

            StringBuilder sb = new StringBuilder(256);

            // if there is an AS suffix, then entire select must be in a parens
            if (m_sAs != null)
                sb.Append("(");

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

            sb = new StringBuilder(256);

            sb.Append(m_sw.GetWhere(sBaseForWhere));
            if (m_sGroupBy != null)
            {
                sb.Append(" GROUP BY ");
                sb.Append(m_sw.ExpandAliases(m_sGroupBy));
            }

            if (m_sOrderBy != null)
            {
                sb.Append(" ORDER BY ");
                sb.Append(m_sw.ExpandAliases(m_sOrderBy));
            }

            if (m_sAs != null)
            {
                sb.Append(") AS ");
                sb.Append(m_sAs);
            }

            return sb.ToString();
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

        public void AddAs(string sAs)
        {
            m_sAs = sAs;
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


        /* A D D  G R O U P  B Y */
        /*----------------------------------------------------------------------------
			%%Function: AddGroupBy

			add a group by clause, expanding aliases.			
		----------------------------------------------------------------------------*/
        public void AddGroupBy(string s)
        {
            s = m_sw.ExpandAliases(s);

            m_sGroupBy = s;
        }

    }
}