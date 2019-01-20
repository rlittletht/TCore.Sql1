using System;

namespace TCore
{
    // ===============================================================================
    //  B H  S Q L  I N N E R  J O I N
    // ===============================================================================
    public class SqlInnerJoin
    {
        string m_sJoin;
        SqlWhere m_sw;

        public override string ToString()
        {
            return String.Format("INNER JOIN {0} ON {1}", m_sJoin, m_sw.GetClause());
        }

        public SqlInnerJoin(string sJoin, SqlWhere swJoin)
        {
            m_sJoin = sJoin;
            m_sw = swJoin;
        }
    }
}