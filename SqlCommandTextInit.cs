using System.Collections.Generic;

namespace TCore;

public class SqlCommandTextInit
{
    public string CommandText { get; set; }
    public Dictionary<string, string> Aliases { get; set; }

    public SqlCommandTextInit(string text, Dictionary<string, string> aliases = null)
    {
        CommandText = text;
        Aliases = aliases;
    }
}
