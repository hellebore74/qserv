/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_REPLICA_DATABASEMYSQL_H
#define LSST_QSERV_REPLICA_DATABASEMYSQL_H

/**
 * This header represents a C++ wrapper for the MySQL C language library.
 * The primary class of this API is Connection.
 *
 * @see class Connection
 *
 * Other public classes of the API, such as class Row, specific exception
 * classes, as well as some others, are defined in separate headers
 * included from this one:
 *
 * DatabaseMySQLExceptions.h
 * DatabaseMySQLTypes.h
 * DatabaseMySQLRow.h
 */

// System headers
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

// Third party headers
#include <mysql/mysql.h>

// Qserv headers
#include "replica/Common.h"
#include "replica/DatabaseMySQLExceptions.h"
#include "replica/DatabaseMySQLTypes.h"
#include "replica/DatabaseMySQLRow.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ProtocolResponseSqlField;
}}} // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {
namespace database {
namespace mysql {

/**
 * Class Connection provides the main API to the database.
 */
class Connection : public std::enable_shared_from_this<Connection> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<Connection> Ptr;

    /// @return value of the corresponding MySQL variable set for a session
    static unsigned long max_allowed_packet();

    /**
     * Connect to the MySQL service with the specified parameters and if successfully
     * connected return a pointer to the Connection object. Otherwise an exception will
     * be thrown.
     *
     * A behavior of a connector created by the method depends on default values
     * of Configuration parameters returned by Configuration::databaseAllowReconnect()
     * and Configuration::databaseConnectTimeoutSec(). If the automatic reconnect is
     * allowed then multiple connection attempts to a database service can be made
     * before the connection timeout expires or until some problem which can't be
     * resolved with the allowed connection retries happens.
     *
     * @note
     *    MySQL auto-commits are disabled
     *
     * @note
     *    MySQL automatic re-connects are not allowed because this Connector class
     *    implements its own protocol for reconnects (when allowed)
     *
     *
     * Here is an example of using this method to establish a connection:
     * @code
     *
     *    BlockPost delayBetweenReconnects(1000,2000);
     *    ConnectionParams params = ...;
     *    Connection::Ptr conn;
     *    do {
     *        try {
     *            conn = Connection::open(params);
     *            
     *        } catch (ConnectError const& ex) {
     *            cerr << "connection attempt failed: " << ex.what << endl;
     *            delayBetweenReconnects.wait();
     *            cerr << "reconnecting..." << endl;
     *            
     *        } catch (ConnectTimeout const& ex) {
     *            cerr << "connection attempt expired after: " << ex.timeoutSec() << " seconds "
     *                 << "due to: " << ex.what << endl;
     *            throw;
     *            
     *        } catch (Error const& ex) {
     *            cerr << "connection attempt failed: " << ex.what << endl;
     *            throw;
     *        }
     *   } while (nullptr != conn);
     *
     * @code
     *
     * @param connectionParams
     *    parameters of a connection
     *
     * @return
     *    a valid object if the connection attempt succeeded (no nullptr
     *    to be returned under any circumstances)
     * 
     * @throws ConnectTimeout
     *    the exception is thrown only if the automatic reconnects
     *    are allowed to indicate that connection attempts to a server
     *    failed to be established within the specified timeout
     *
     * @throws ConnectError
     *    the exception is thrown if automatic reconnets are not allowed
     *    to indicate that the only connection attempt to a server failed
     *
     * @throws Error - for any other database errors
     *
     * @see Configuration::databaseAllowReconnect()
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Connection::open2()
     */
    static Ptr open(ConnectionParams const& connectionParams);

    /**
     * The factory method allows to override default values of the corresponding
     * connection management options of the Configuration.
     *
     * @note
     *   if the timeout is set to 0 (the default value) and if reconnects are
     *   allowed then the method will assume a global value defined by
     *   the Configuration parameter: Configuration::databaseConnectTimeoutSec()
     *
     * @note
     *   the same value of the timeout would be also assumed if the connection
     *   is lost when executing queries or pulling the result sets.
     * 
     * @param connectionParams
     *
     * @param allowReconnects
     *   if set to 'true' then multiple reconnection attempts will be allowed
     *   
     * @param connectTimeoutSec
     *   maximum number of seconds to wait before a connection with a database
     *   server is established.
     *
     * @return
     *  a valid object if the connection attempt succeeded (no nullptr
     *  to be returned under any circumstances)
     *
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Connection::open()
     */
    static Ptr open2(ConnectionParams const& connectionParams,
                     bool allowReconnects=false,
                     unsigned int connectTimeoutSec=0);

    // Default construction and copy semantics are prohibited

    Connection() = delete;
    Connection(Connection const&) = delete;
    Connection& operator=(Connection const&) = delete;

    ~Connection();

    /// @return maximum amount of time to wait while making reconnection attempts
    unsigned int connectTimeoutSec() const { return _connectTimeoutSec; }


    std::string escape(std::string const& str) const;

    std::string charSetName() const;

    // -------------------------------------------------
    // Helper methods for simplifying query preparation
    // -------------------------------------------------

    template <typename T>
    T           sqlValue(T const&            val) const { return val; }
    std::string sqlValue(std::string const&  val) const { return "'" + escape(val) + "'"; }
    std::string sqlValue(char const*         val) const { return sqlValue(std::string(val)); }
    std::string sqlValue(DoNotProcess const& val) const { return val.name; }
    std::string sqlValue(Keyword      const& val) const { return val.name; }
    std::string sqlValue(Function     const& val) const { return val.name; }

    std::string sqlValue(std::vector<std::string> const& coll) const;

    /**
     * The function replaces the "conditional operator" of C++ in SQL statements
     * generators. Unlike the standard operator this function allows internal
     * type switching while producing a result of a specific type.
     *
     * @return
     *   an object which doesn't require any further processing
     */
    DoNotProcess nullIfEmpty(std::string const& val) {
        return val.empty() ? DoNotProcess(Keyword::SQL_NULL) : DoNotProcess(sqlValue(val));
    }

    // Generator: ([value [, value [, ... ]]])
    // Where values of the string types will be surrounded with single quotes

    /// The end of variadic recursion
    void sqlValues(std::string& sql) const { sql += ")"; }

    /// The next step in the variadic recursion when at least one value is
    /// still available
    template <typename T,
              typename ...Targs>
    void sqlValues(std::string& sql,
                   T            val,
                   Targs...     Fargs) const {

        bool const last = sizeof...(Fargs) - 1 < 0;
        std::ostringstream ss;
        ss << (sql.empty() ? "(" : (last ? "" : ",")) << sqlValue(val);
        sql += ss.str();

        // Recursively keep drilling down the list of arguments with one
        // argument less.
        sqlValues(sql, Fargs...);
    }

    /**
     * Turn values of variadic arguments into a valid SQL representing a set of
     * values to be insert into a table row. Values of string types 'std::string const&'
     * and 'char const*' will be also escaped and surrounded by single quote.
     *
     * For example, the following call:
     * @code
     *   sqlPackValues("st'r", std::string("c"), 123, 24.5);
     * @code
     *
     * This will produce the following output:
     * @code
     *   ('st\'r','c',123,24.5)
     * @code
     */
    template <typename...Targs>
    std::string sqlPackValues(Targs... Fargs) const {
        std::string sql;
        sqlValues(sql, Fargs...);
        return sql;
    }

    /**
     * Generate an SQL statement for inserting a single row into the specified
     * table based on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. Arguments of
     * types 'std::string' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * @param tableName
     *   the name of a table
     *
     * @param Fargs
     *   the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlInsertQuery(std::string const& tableName,
                               Targs...           Fargs) const {
        std::ostringstream qs;
        qs  << "INSERT INTO " << sqlId(tableName) << " "
            << "VALUES "      << sqlPackValues(Fargs...);
        return qs.str();
    }

    /// Return a string representing a built-in MySQL function for the last
    /// insert auto-incremented identifier: LAST_INSERT_ID()
    std::string sqlLastInsertId() const { return "LAST_INSERT_ID()"; }

    // ----------------------------------------------------------------------
    // Generator: [`column` = value [, `column` = value [, ... ]]]
    // Where values of the string types will be surrounded with single quotes

    /// Return a non-escaped and back-tick-quoted string which is meant
    /// to be an SQL identifier.
    std::string sqlId(std::string const& str) const { return "`" + str + "`"; }

    /// @return a back-ticked identifier of a MySQL partition for the given "super-transaction"
    std::string sqlPartitionId(TransactionId transactionId) const {
        return sqlId("p" + std::to_string(transactionId));
    }

    /**
     * Generate and return an SQL expression for a binary operator applied
     * over a pair of a simple identifier and a value.
     *
     * @param col  the name of a column on the LHS of the expression
     * @param val  RHS value of the binary operation
     * @param op   binary operator to be applied to both above
     *
     * @return "<col> <binary operator> <value>" where column name will be
     * surrounded by back ticks, and  values of string types will be escaped
     * and surrounded by single quotes.
     */
    template <typename T>
    std::string sqlBinaryOperator(std::string const& col,
                                  T const&           val,
                                  char const*        op) const {
        std::ostringstream ss;
        ss << sqlId(col) << op << sqlValue(val);
        return ss.str();
    }
    
    /**
     * @return "<quoted-col> = <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "=");
    }

    /**
     * @return "<quoted-col> != <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlNotEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "!=");
    }

    /**
     * @return "<quoted-col> < <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLess(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "<");
    }

    /**
     * @return "<quoted-col> <= <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlLessOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, "<=");
    }

    /**
     * @return "<quoted-col> > <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreater(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, ">");
    }

    /**
     * @return "<quoted-col> => <escaped-quoted-value>"
     * @see Connection::sqlBinaryOperator()
     */
    template <typename T>
    std::string sqlGreaterOrEqual(std::string const& col, T const& val) const {
        return sqlBinaryOperator(col, val, ">=");
    }

    /// The base (the final function) to be called
    void sqlPackPair(std::string&) const {}

    /// Recursive variadic function (overloaded for column names given as std::string)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string&             sql,
                     std::pair<std::string,T> colVal,
                     Targs...                 Fargs) const {

        std::string const& col = colVal.first;
        T const&           val = colVal.second;

        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }


    /// Recursive variadic function (overloaded for column names given as char const*)
    template <typename T, typename...Targs>
    void sqlPackPair(std::string&             sql,
                     std::pair<char const*,T> colVal,
                     Targs...                 Fargs) const {

        std::string const  col = colVal.first;
        T const&           val = colVal.second;

        std::ostringstream ss;
        ss << (sql.empty() ? "" : (sizeof...(Fargs) - 1 < 0 ? "" : ",")) << sqlEqual(col, val);
        sql += ss.str();
        sqlPackPair(sql, Fargs...);
    }

    /**
     * Pack pairs of column names and their new values into a string which can be
     * further used to form SQL statements of the following kind:
     *
     *   UPDATE <table> SET <packed-pairs>
     *
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * For example, the following call:
     * @code
     *     sqlPackPairs (
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123),
     *         std::make_pair("fk_id", Function::LAST_INSERT_ID));
     * @code
     * will produce the following string content:
     * @code
     *     `col1`='st\'r',`col2`="c",`col3`=123,`fk_id`=LAST_INSERT_ID()
     * @code
     *
     * @param Fargs
     *   the variadic list of values to be inserted
     */
    template <typename...Targs>
    std::string sqlPackPairs(Targs... Fargs) const {
        std::string sql;
        sqlPackPair(sql, Fargs...);
        return sql;
    }

    /**
     * Return:
     *
     *   `col` IN (<val1>,<val2>,<val3>,,,)
     *
     * NOTES:
     * - the column name will be surrounded by back ticks
     * - values of string types will be escaped and surrounded by single quotes
     *
     * @param col
     *   the name of a column
     *
     * @param values
     *   an iterable collection of values
     */
    template <typename T>
    std::string sqlIn(std::string const& col,
                      T const&           values) const {
        std::ostringstream ss;
        ss << sqlId(col) << " IN (";
        int num=0;
        for (auto&& val: values)
            ss << (num++ ? "," : "") << sqlValue(val);
        ss << ")";
        return ss.str();
    }

    /**
     * Generate an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     *
     * NOTES:
     * - The method allows any number of arguments and any types of value types.
     * - Values types 'std::string' and 'char*' will be additionally escaped and
     *   surrounded by single quotes as required by the SQL standard.
     * - The column names will be surrounded with back-tick quotes.
     *
     * For example:
     * @code
     *     connection->sqlSimpleUpdateQuery (
     *         "table",
     *         sqlEqual("fk_id", Function::LAST_INSERT_ID),
     *         std::make_pair("col1",  "st'r"),
     *         std::make_pair("col2",  std::string("c")),
     *         std::make_pair("col3",  123));
     * @code
     * This will generate the following query (extra newline symbols are added
     * to me this example a bit easy to read:
     * @code
     *     UPDATE `table`
     *     SET `col1`='st\'r',
     *         `col2`="c",
     *         `col3`=123
     *     WHERE
     *       `fk_id`=LAST_INSERT_ID()
     * @code
     *
     * @param tableName
     *   the name of a table
     *
     * @param condition
     *   the optional condition for selecting rows to be updated
     *
     * @param Fargs
     *   the variadic list of values to be inserted
     *
     * @return
     *   well-formed SQL statement
     */
    template <typename...Targs>
    std::string sqlSimpleUpdateQuery(std::string const& tableName,
                                     std::string const& condition,
                                     Targs...           Fargs) const {
        std::ostringstream qs;
        qs  << "UPDATE " << sqlId(tableName)       << " "
            << "SET "    << sqlPackPairs(Fargs...) << " "
            << (condition.empty() ? "" : "WHERE " + condition);
        return qs.str();
    }

    ///  @return the status of the transaction
    bool inTransaction() const { return _inTransaction; }

    /**
     * Start the transaction
     *
     * @return
     *   smart pointer to self to allow chained calls
     *
     * @throws std::logic_error
     *   if the transaction was already been started
     *
     * @throws Error
     *   for any other MySQL specific errors
     */
    Connection::Ptr begin();

    /**
     * Commit the transaction
     *
     * @return
     *   smart pointer to self to allow chained calls
     * 
     * @throws std::logic_error
     *   if the transaction was not started
     *
     * @throws Error
     *   for any other MySQL specific errors
     */
    Connection::Ptr commit();

    /**
     * Rollback the transaction
     *
     * @return
     *   smart pointer to self to allow chained calls
     * 
     * @throws std::logic_error
     *   if the transaction was not started
     * 
     * @throws Error
     *   for any other MySQL specific errors
     */
    Connection::Ptr rollback();

    /**
     * Execute the specified query and initialize object context to allow
     * a result set extraction.
     *
     * @param query
     *   query to be executed
     * 
     * @return
     *   smart pointer to self to allow chained calls
     * 
     * @throws std::invalid_argument
     *    for empty query strings
     *
     * @throws DuplicateKeyError
     *   for attempts to insert rows with duplicate keys
     * 
     * @throws Error
     *   for any other MySQL specific errors
     */
    Connection::Ptr execute(std::string const& query);

    /**
     * Execute an SQL statement for inserting a new row into a table based
     * on a variadic list of values to be inserted. The method allows
     * any number of arguments and any types of argument values. Arguments of
     * types 'std::string' and 'char*' will be additionally escaped and surrounded by
     * single quotes as required by the SQL standard.
     *
     * The effect:
     *
     *   INSERT INTO <table> VALUES (<packed-values>)
     *
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of the method.
     *
     * @see Connection::sqlInsertQuery()
     *
     * @param tableName
     *   the name of a table
     *
     * @param Fargs
     *   the variadic list of values to be inserted
     *
     * @return
     *   smart pointer to self to allow chained calls
     *
     * @throws DuplicateKeyError
     *   for attempts to insert rows with duplicate keys
     *
     * @throws Error
     *   for any other MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeInsertQuery(std::string const& tableName,
                                       Targs...           Fargs) {

        return execute(sqlInsertQuery(tableName,
                                      Fargs...));
    }

    /**
     * Execute an SQL statement for updating select values of table rows
     * where the optional condition is met. Fields to be updated and their new
     * values are passed into the method as variadic list of std::pair objects.
     *
     * The effect:
     *
     *   UPDATE <table> SET <packed-pairs> [WHERE <condition>]
     *
     * ATTENTION: the method will *NOT* start a transaction, neither it will
     * commit the one in the end. Transaction management is a responsibility
     * of a caller of the method.
     *
     * @see Connection::sqlSimpleUpdateQuery()
     *
     * @param tableName
     *   the name of a table
     *
     * @param whereCondition
     *   the optional condition for selecting rows to be updated
     *
     * @param Fargs
     *   the variadic list of column-value pairs to be updated
     *
     * @return
     *   smart pointer to self to allow chained calls
     *
     * @throws std::invalid_argument
     *   for empty query strings
     *
     * @throws Error
     *   for any MySQL specific errors
     */
    template <typename...Targs>
    Connection::Ptr executeSimpleUpdateQuery(std::string const& tableName,
                                             std::string const& condition,
                                             Targs...           Fargs) {

        return execute(sqlSimpleUpdateQuery(tableName,
                                            condition,
                                            Fargs...));
    }

    /**
     * Execute a user-supplied algorithm which could be retried the specified
     * number of times (or until a given timeout expires) if a connection to
     * a server is lost and re-established before the completion of the algorithm.
     * The number of allowed auto-reconnects and the timeout are controlled by
     * the corresponding parameters of the method.
     *
     * Notes:
     * - in case of reconnects and retries the failed transaction will be aborted
     * - it's up to a user script to begin and commit a transaction as needed
     * - it's up to a user script to take care of side effects if the script will run
     * more than once
     *
     * Example:
     *   @code
     *     Configuration::setDatabaseConnectTimeoutSec(60);
     *     ConnectionParams params = ... ;
     *     Connection::Ptr conn = Connection::open(params);
     *     try {
     *         conn->execute(
     *             [](Connection::Ptr conn) {
     *                 conn->begin();
     *                 conn->execute("SELECT ...");
     *                 conn->execute("INSERT ...");
     *                 conn->commit();
     *             },
     *             10                        // 10 extra attempts before to fail
     *             2 * connectionTimeoutSec  // no longer that 120 seconds
     *         );
     *     } catch (ConnectError const& ex) {
     *         cerr << "you only made one failed attempt because "
     *              << "no automatic reconnects were allowed. Open your connection "
     *              << "with factory method Connection::openWait()" << endl;
     *     } catch (ConnectTimeout const& ex) {
     *         cerr << "you have exausted the maximum allowed number of retries "
     *              << "within the specified (or implicitly assumed) "
     *                 "timeout: " << ex.timeoutSec() << endl;
     *     } catch (Error const& ex) {
     *         cerr << "failed due to an unrecoverable error: " << ex.what() 
     *     }
     *   @code
     *
     *
     * @param script
     *   user-provided function (the callable) to execute
     *
     * @param maxReconnects
     *   (optional) maximum number of reconnects allowed
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseMaxReconnects().
     *
     * @param timeoutSec
     *   (optional) the maximum duration of time allowed for
     *   the procedure to wait before a connection will be established.
     *   If 0 is passed as a value pf the parameter then the default
     *   value corresponding configuration parameter will be
     *   assumed: Configuration::databaseConnectTimeoutSec().
     *
     * @throws std::invalid_argument
     *   if 'nullptr' is passed in place of 'script'
     *
     * @throws ConnectError
     *   failed to establish a connection if connection was
     *   open with fuctory method Connection::open()
     *
     * @throws ConnectTimeout
     *   failed to establish a connection within a timeout
     *   if a connection was open with functory method
     *   Connection::openWait()
     *
     * @throws MaxReconnectsExceeded
     *   multiple failed (due to connecton losses and subsequent reconnects)
     *   attempts to execute the user function. And the number of the attempts
     *   exceeded a limit set by parameter 'maxReconnects'.
     * 
     * @throws Error
     *   or any MySQL specific errors. You may also
     *   catch for specific subclasses (other than ConnectError
     *   or ConnectTimeout) of that class if needed.
     *
     * @return
     *   pointer to the same connector against which the method was invoked
     *   in case of successful commpletion of the requested operaton.
     *
     * @see Configuration::databaseMaxReconnects()
     * @see Configuration::setDatabaseMaxReconnects()
     * 
     * @see Configuration::databaseConnectTimeoutSec()
     * @see Configuration::setDatabaseConnectTimeoutSec()
     * 
     * @see Connection::open()
     */
    Connection::Ptr execute(std::function<void(Ptr)> const& script,
                            unsigned int maxReconnects=0,
                            unsigned int timeoutSec=0);

    /**
     * This is just a convenience method for a typical use case
     *
     * @note
     *   it's up to the 'updateScript' to rollback a previous transaction
     *   if needed.
     */
    Connection::Ptr executeInsertOrUpdate(std::function<void(Ptr)> const& insertScript,
                                          std::function<void(Ptr)> const& updateScript,
                                          unsigned int maxReconnects=0,
                                          unsigned int timeoutSec=0) {
        try {
            return execute(insertScript, maxReconnects, timeoutSec);
        } catch (database::mysql::DuplicateKeyError const&) {
            return execute(updateScript, maxReconnects, timeoutSec);
        }
    }

    /**
     * @return
     *   'true' if the last successful query returned a result set
     *   (even though it may be empty)
     */
    bool hasResult() const;

    /**
     * @return
     *  names of the columns from the current result set
     *
     * @note
     *   columns are returned exactly in the same order they were
     *   requested in the corresponding query.
     *
     * @throws std::logic_error
     *   if no SQL statement has ever been executed, or
     *   if the last query failed.
     */
    std::vector<std::string> const& columnNames() const;

    /**
     * @return
     *  the number of columns in the current result set
     *
     * @throws std::logic_error
     *   if no SQL statement has ever been executed, or
     *   if the last query failed.
     */
    size_t numFields() const;

    /**
     * Fill a Protobuf object representing a field
     * 
     * @note
     *   The method can be called only upon a successful completion of a query
     *   which has a result set. Otherwise it will throw an exception.
     *
     * @see mysql_fetch_field()
     * @see database::mysql::Connection::hasResult
     *
     * @param ptr
     *   a pointer to the Protobuf object to be populated
     * 
     * @param idx
     *   a relative (0 based) index of the field in a result set
     *
     * @throws std::logic_error
     *   if no SQL statement has ever been executed, or
     *   if the last query failed.
     * 
     * @throws std::out_of_range
     *   if the specified index exceed the maximum index of a result set.
     */
    void exportField(ProtocolResponseSqlField* ptr, size_t idx) const;

    /**
     * Move the iterator to the next (first) row of the current result set
     * and if the iterator is not beyond the last row then initialize an object
     * passed as a parameter.
     *
     * ATTENTION: objects initialized upon the successful completion
     * of the method are valid until the next call to the method or before
     * the next query. Hence the safe practice for using this method to iterate
     * over a result set would be:
     *
     *   @code
     *     Connection::Ptr conn = Connection::connect(...);
     *     conn->execute ("SELECT ...");
     *
     *     Row row;
     *     while (conn->next(row)) {
     *         // Extract data from 'row' within this block
     *         // before proceeding to the next row, etc.
     *     }
     *   @code
     *
     * @return
     *   'true' if the row was initialized or 'false' if past the last row
     *   in the result set.
     *
     * @throws std::logic_error
     *    if no SQL statement has ever been executed, or
     *    if the last query failed.
     */
    bool next(Row& row);

    /**
     * The convenience method is for executing a query from which a single value
     * will be extracted (typically a PK). Please, read the notes below.
     *
     * @note
     *   By default the method requires a result set to have 0 or 1 rows.
     *   If the result set has more than one row exception std::logic_error
     *   will be thrown.
     *
     * @note
     *   The previously mentioned requirement can be relaxed by setting
     *   a value of the optional parameter 'noMoreThanOne' to 'false'.
     *   In that case a value from the very first row will be extracted.
     *
     * @note
     *   If a result set is empty the method will throw EmptyResultSetError
     *
     * @note
     *   If the field has 'NULL' the method will return 'false'
     *
     * @note
     *   If the conversion to a proposed type will fail the method will
     *   throw InvalidTypeError
     *
     * @param query
     *   a query to be executed
     *
     * @param col
     *   the name of a column from which to extract a value
     * 
     * @param val
     *   a value to be set (unless the field contains NULL)
     * 
     * @param noMoreThanOne
     *   flag (if set) forcing the above explained behavior
     *
     * @return
     *   'true' if the value is not NULL
     */
    template <typename T>
    bool executeSingleValueSelect(std::string const& query,
                                  std::string const& col,
                                  T&                 val,
                                  bool               noMoreThanOne=true) {
        execute(query);
        if (not hasResult()) {
            throw EmptyResultSetError(
                "DatabaseMySQL::executeSingleValueSelect()  result set is empty");
        }

        bool isNotNull;
        size_t numRows = 0;

        Row row;
        while (next(row)) {

            // Only the very first row matters
            if (not numRows) isNotNull = row.get(col, val);

            // have to read the rest of the result set to avoid problems with the MySQL
            // protocol
            ++numRows;
        }
        if ((1 == numRows) or not noMoreThanOne) return isNotNull;

        throw std::logic_error(
                "DatabaseMySQL::executeSingleValueSelect()  result set has more than 1 row");
    }

private:

    /**
     * @see Connection::open()
     * @see Connection::open2()
     */
    Connection(ConnectionParams const& connectionParams,
               unsigned int connectTimeoutSec);

    /**
     * Keep trying to connect to a server until either a timeout expires, or
     * some unrecoverable failure happens while trying to establish a connection.
     *
     * @throws ConnectTimeout
     *   failed to establish a connection within a timeout
     *
     * @throws Error
     *   other problem when preparing or establishing a connection
     */
    void _connect();

    /**
     * Make exactly one attempt to establish a connection
     *
     * @throws ConnectError
     *   connection to a server failed
     *
     * @throws Error
     *   other problem when preparing or establishing a connection
     */
    void _connectOnce();

    /**
     * The method is called to process the last error, reconnect if needed (and allowed), etc.
     *
     * @param context
     *   context from which this method was called
     *
     * @param instantAutoReconnect
     *   (optional) flag allowing to make instant reconnects for
     *   qualified error conditions
     *
     * @throws std::logic_error
     *   if the method is called after no actual error happened
     *
     * @throws Reconnected
     *   after a successful reconnection has happened
     *
     * @throws ConnectError
     *   connection to a server failed
     *
     * @throws DuplicateKeyError
     *   after the last statement attempted to violate
     *   the corresponding key constraint
     *
     * @throws Error
     *   for some other error not listed above
     */
    void _processLastError(std::string const& context,
                           bool instantAutoReconnect=true);

    /**
     * The method is to ensure that the transaction is in the desired state.
     *
     * @param inTransaction
     *   the desired state of the transaction
     */
    void _assertTransaction(bool inTransaction) const;

    /**
     * The method is to ensure that a proper query context is set and
     * its result set can be explored.
     *
     * @throw Error
     *   if the connection is not established or no prior query was made
     */
    void _assertQueryContext() const;


    /// Sequence of the connector identifiers
    static std::atomic<size_t> _nextId;

    /// Unique identifier of a connector
    size_t const _id;

    /// Parameters of the connection
    ConnectionParams const _connectionParams;

    /// Maximum amount of time to wait while making reconnection attempts
    unsigned int _connectTimeoutSec;

    /// The last SQL statement
    std::string _lastQuery;

    /// Transaction status
    bool _inTransaction;

    // Connection

    MYSQL* _mysql;
    unsigned long _mysqlThreadId;       // thread ID of the current connection
    unsigned long _connectionAttempt;   // the counter of attempts between successful reconnects

    // Last result set

    MYSQL_RES*   _res;
    MYSQL_FIELD* _fields;

    size_t _numFields;

    std::vector<std::string> _columnNames;

    std::map<std::string, size_t> _name2index;

    std::string _charSetName;   // of the current connection

    // Get updated after fetching each row of the result set

    MYSQL_ROW _row;     // must be cached here to ensure its lifespan
                        // while a client will be processing its content.
};


/**
 * Class ConnectionPool manages a pool of the similarly configured persistent
 * database connection. The number of connections is determined by the corresponding
 * Configuration parameter. Connections will be added to the pool (up to that limit)
 * on demand. This ensures that the class constructor is not blocking in case if
 * (or while) the corresponding MySQL/MariaDB service is not responding.
 * 
 * @note this class is meant to be used indirectly by passing its instances
 * to the constructor of class ConnectionHandler.
 *
 * @see class ConnectionHandler
 */
class ConnectionPool {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<ConnectionPool> Ptr;

    /**
     * The static factory object creates a pool and sets the maximum number
     * of connections.
     * 
     * @note this is a non-blocking method. In particular, No connection attempts
     * will be made by this method.
     *
     * @param params  parameters of the connections
     * @param maxConnections  the maximum number of connections managed by the pool
     * @return smart pointer to the newly created object
     */
    static Ptr create(ConnectionParams const& params,
                      size_t maxConnections);

    // Copy semantics and the default construction is prohibited

    ConnectionPool() = delete;
    ConnectionPool(ConnectionPool const&) = delete;
    ConnectionPool& operator=(ConnectionPool const&) = delete;

    /**
     * Allocate (and open a new if required/possible) connection
     *
     * @note the requester must return the service back after it's no longer needed.
     * @return pointer to the allocated connector
     * @see ConnectionPool::release()
     */
    Connection::Ptr allocate();

    /**
     * Return a connection object back into the pool of the available ones.
     *
     * @param conn  connection object to be returned back
     * @throws std::logic_error if the object was not previously allocated
     * @see ConnectionPool::allocate()
     */
    void release(Connection::Ptr const& conn);

private:

    ConnectionPool(ConnectionParams const& params,
                   size_t maxConnections);

    // Input parameters

    ConnectionParams const _params;
    size_t const _maxConnections;

    /// Connection objects which are available
    std::list<Connection::Ptr> _availableConnections;

    /// Connection objects which are in use
    std::list<Connection::Ptr> _usedConnections;

    /// The mutex for enforcing thread safety of the class's public API and
    /// internal operations. The mutex is locked by methods ConnectionPool::allocate()
    /// and ConnectionPool::release() when moving connection objects between
    /// the lists (defined above).
     mutable std::mutex _mtx;

    /// The condition variable for notifying client threads waiting for the next
    /// available connection.
    std::condition_variable _available;
};


/**
 * Class ConnectionHandler implements the RAII method of handling database
 * connection.
 */
class ConnectionHandler {

public:

    /**
     * The default constructor will initialize the connection pointer
     * with 'nullptr'
     */
    ConnectionHandler() = default;

    /**
     * Construct with a connection
     *
     * @param conn_  connection to be watched and managed
     */
    explicit ConnectionHandler(Connection::Ptr const& conn_);

    /**
     * Construct with a pointer to a connection pool for allocating
     * a connection. The connection will get released by the destructor.
     *
     * @param pool  connection pool to acquire persistent connections
     */
    explicit ConnectionHandler(ConnectionPool::Ptr const& pool);

    // Copy semantics is prohibited

    ConnectionHandler(ConnectionHandler const&) = delete;
    ConnectionHandler& operator=(ConnectionHandler const&) = delete;

    /**
     * The destructor will rollback a transaction if any was started at
     * a presence of a connection.
     */
    ~ConnectionHandler();

    /// The smart reference to the connector object (if any))
    Connection::Ptr conn;

private:

    /// The smart reference to the connector pool object (if any))
    ConnectionPool::Ptr _pool;
};


}}}}} // namespace lsst::qserv::replica::database::mysql

#endif // LSST_QSERV_REPLICA_DATABASEMYSQL_H
