
using System;
using TCore.Exceptions;

namespace TCore
{
    class SqlErrorMessages
    {
        public const string Unknown = "Unknown exception";
    }

    // Basic exception for our WebApi to allow us to differentiate exceptions
    public class SqlException : TcException
    {
        public SqlException() : base(Guid.Empty) { }
        public SqlException(string errorMessage) : base(errorMessage) { }
        public SqlException(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public SqlException(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public SqlException(Guid crids, string errorMessage, Exception innerException) : base(crids, innerException, errorMessage) { }
    }

    // Exception when the result set did not contain a single row
    public class SqlExceptionNotSingleRow : SqlException
    {
    }

    public class SqlExceptionNoResults : SqlException
    {
    }

}