
using System;
using TCore.Exceptions;

namespace TCore
{
    class SqlErrorMessages
    {
        public const string Unknown = "Unknown exception";
    }

    // Basic exception for our WebApi to allow us to differentiate exceptions
    public class TcSqlException : TcException
    {
        public TcSqlException() : base(Guid.Empty) { }
        public TcSqlException(Guid crids) : base(crids) { }
        public TcSqlException(string errorMessage) : base(errorMessage) { }
        public TcSqlException(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public TcSqlException(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public TcSqlException(Guid crids, Exception innerException, string errorMessage) : base(crids, innerException, errorMessage) { }
    }

    // Exception when the result set did not contain a single row
    public class TcSqlExceptionNotSingleRow : TcSqlException
    {
        public TcSqlExceptionNotSingleRow() : base(Guid.Empty) { }
        public TcSqlExceptionNotSingleRow(Guid crids) : base(crids) { }
        public TcSqlExceptionNotSingleRow(string errorMessage) : base(errorMessage) { }
        public TcSqlExceptionNotSingleRow(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public TcSqlExceptionNotSingleRow(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public TcSqlExceptionNotSingleRow(Guid crids, Exception innerException, string errorMessage) : base(crids, innerException, errorMessage) { }
    }

    public class TcSqlExceptionNoResults : TcSqlException
    {
        public TcSqlExceptionNoResults() : base(Guid.Empty) { }
        public TcSqlExceptionNoResults(Guid crids) : base(crids) { }
        public TcSqlExceptionNoResults(string errorMessage) : base(errorMessage) { }
        public TcSqlExceptionNoResults(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public TcSqlExceptionNoResults(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public TcSqlExceptionNoResults(Guid crids, Exception innerException, string errorMessage) : base(crids, innerException, errorMessage) { }
    }

    public class TcSqlExceptionInTransaction : TcSqlException
    {
        public TcSqlExceptionInTransaction() : base(Guid.Empty) { }
        public TcSqlExceptionInTransaction(Guid crids) : base(crids) { }
        public TcSqlExceptionInTransaction(string errorMessage) : base(errorMessage) { }
        public TcSqlExceptionInTransaction(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public TcSqlExceptionInTransaction(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public TcSqlExceptionInTransaction(Guid crids, Exception innerException, string errorMessage) : base(crids, innerException, errorMessage) { }
    }

    public class TcSqlExceptionNotInTransaction : TcSqlException
    {
        public TcSqlExceptionNotInTransaction() : base(Guid.Empty) { }
        public TcSqlExceptionNotInTransaction(Guid crids) : base(crids) { }
        public TcSqlExceptionNotInTransaction(string errorMessage) : base(errorMessage) { }
        public TcSqlExceptionNotInTransaction(string errorMessage, Exception innerException) : base(errorMessage, innerException) { }
        public TcSqlExceptionNotInTransaction(Guid crids, string errorMessage) : base(crids, errorMessage) { }
        public TcSqlExceptionNotInTransaction(Guid crids, Exception innerException, string errorMessage) : base(crids, innerException, errorMessage) { }
    }

}