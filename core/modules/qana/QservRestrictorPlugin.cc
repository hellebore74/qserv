// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
/**
  * @file
  *
  * @brief QservRestrictorPlugin implementation
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qana/QservRestrictorPlugin.h"

// System headers
#include <algorithm>
#include <deque>
#include <memory>
#include <string>

// Third-party headers
#include "boost/pointer_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssAccess.h"
#include "global/stringTypes.h"
#include "qana/AnalysisError.h"
#include "query/AndTerm.h"
#include "query/BoolFactor.h"
#include "query/BetweenPredicate.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/InPredicate.h"
#include "query/JoinRef.h"
#include "query/PassTerm.h"
#include "query/PassListTerm.h"
#include "query/QsRestrictor.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/ValueFactor.h"
#include "query/ValueExpr.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"


namespace { // File-scope helpers

using namespace lsst::qserv;

std::string const UDF_PREFIX = "scisql_";

LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.QservRestrictorPlugin");

enum RestrictorType { SECONDARY_INDEX_IN =1, SECONDARY_INDEX_NOT_IN,
    SECONDARY_INDEX_BETWEEN, SECONDARY_INDEX_NOT_BETWEEN};


/// RestrictorEntry is a class to contain information about chunked tables.
struct RestrictorEntry {

    RestrictorEntry(std::string const& alias_,
                    StringPair const& chunkColumns_,
                    std::string const& secIndexColumn_)
        : alias(alias_),
          chunkColumns(chunkColumns_),
          secIndexColumn(secIndexColumn_)
        {}
    std::string alias; //< The alias of the chunked table.
    StringPair chunkColumns;
    std::string secIndexColumn;
};


typedef std::pair<std::string, std::string> StringPair;
typedef std::deque<RestrictorEntry> RestrictoryEntryList;


class getTable : public query::TableRef::Func {
public:

    getTable(css::CssAccess& css, RestrictoryEntryList& chunkedTables)
        : _css(css),
          _chunkedTables(chunkedTables) {}

    void operator()(query::TableRef::Ptr t) {
        // FIXME: Modify so we can use TableRef::apply()
        if (!t) {
            throw qana::AnalysisBug("NULL TableRefN::Ptr");
        }
        (*this)(*t);
    }
    virtual void operator()(query::TableRef& t) {
        std::string const& db = t.getDb();
        std::string const& table = t.getTable();

        if (db.empty()
           || !_css.containsDb(db)
           || !_css.containsTable(db, table)) {
            throw qana::AnalysisError("Invalid db/table:" + db + "." + table);
        }
        css::PartTableParams const& partParam = _css.getPartTableParams(db, table);
        // Is table chunked?
        if (!partParam.isChunked()) {
            return; // Do nothing for non-chunked tables
        }
        // Now save an entry for WHERE clause processing.
        std::string alias = t.getAlias();
        if (alias.empty()) {
            // For now, only accept aliased tablerefs (should have
            // been done earlier)
            throw qana::AnalysisBug("Unexpected unaliased table reference");
        }
        std::vector<std::string> pCols = partParam.partitionCols();
        RestrictorEntry se(alias,
                           StringPair(pCols[0], pCols[1]),
                           pCols[2]);
        _chunkedTables.push_back(se);
        query::JoinRefPtrVector& joinRefs = t.getJoins();
        typedef query::JoinRefPtrVector::iterator Iter;
        for (Iter i=joinRefs.begin(), e=joinRefs.end(); i != e; ++i) {
            (*this)((**i).getRight());
        }
    }
    css::CssAccess& _css;
    RestrictoryEntryList& _chunkedTables;
};


template <typename C>
query::FuncExpr::Ptr newFuncExpr(char const fName[],
                                 std::string const& tableAlias,
                                 StringPair const& chunkColumns,
                                 C& c) {
    query::FuncExpr::Ptr fe = std::make_shared<query::FuncExpr>();
    fe->setName(UDF_PREFIX + fName);
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                std::make_shared<query::ColumnRef>("", "", tableAlias, chunkColumns.first))));
    fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newColumnRefFactor(
                std::make_shared<query::ColumnRef>("", "", tableAlias, chunkColumns.second))));

    typename C::const_iterator i;
    for (i = c.begin(); i != c.end(); ++i) {
        fe->params.push_back(
          query::ValueExpr::newSimple(query::ValueFactor::newConstFactor(*i)));
    }
    return fe;
}


/// Restriction generates WHERE clause terms from restriction specs.
/// Borrowed from older parsing framework.
class Restriction {
public:
    Restriction(query::QsRestrictor const& r)
        : _name(r._name) {
        _setGenerator(r);
    }

    query::BoolFactor::Ptr generate(RestrictorEntry const& e) {
        return (*_generator)(e);
    }

    class Generator {
    public:
        virtual ~Generator() {}
        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) = 0;
    };

private:
    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      StringVector const& params_)
            :  fName(fName_), paramCount(paramCount_), params(params_) {
            if (paramCount_ == USE_STRING) {
                // Convert param list to one quoted string.
                // This bundles a variable-sized list into a single
                // parameter to work with the MySQL UDF facility.
            }
        }

        virtual query::BoolFactor::Ptr operator()(RestrictorEntry const& e) {
            query::BoolFactor::Ptr newFactor =
                    std::make_shared<query::BoolFactor>();
            query::BoolFactorTerm::PtrVector& terms = newFactor->_terms;
            query::CompPredicate::Ptr cp =
                    std::make_shared<query::CompPredicate>();
            std::shared_ptr<query::FuncExpr> fe =
                newFuncExpr(fName, e.alias, e.chunkColumns, params);
            cp->left =
                query::ValueExpr::newSimple(query::ValueFactor::newFuncFactor(fe));
            cp->op = query::CompPredicate::EQUALS_OP;
            cp->right = query::ValueExpr::newSimple(
                           query::ValueFactor::newConstFactor("1"));
            terms.push_back(cp);
            return newFactor;
        }

        char const* const fName;
        int const paramCount;
        StringVector const& params;
        static const int USE_STRING = -999;
    };

    void _setGenerator(query::QsRestrictor const& r) {
        if (r._name == "qserv_areaspec_box") {
            _generator = std::make_shared<AreaGenerator>("s2PtInBox",
                                                         4,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_circle") {
            _generator = std::make_shared<AreaGenerator>("s2PtInCircle",
                                                         3,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_ellipse") {
            _generator = std::make_shared<AreaGenerator>("s2PtInEllipse",
                                                         5,
                                                         r._params
                                                         );
        } else if (r._name == "qserv_areaspec_poly") {
            const int use_string = AreaGenerator::USE_STRING;
            _generator = std::make_shared<AreaGenerator>("s2PtInCPoly",
                                                         use_string,
                                                         r._params
                                                         );
        } else {
            throw qana::AnalysisBug("Unmatched restriction spec: " + _name);
        }
    }
    std::string _name;
    std::vector<double> _params;
    std::shared_ptr<Generator> _generator;
};


query::BoolTerm::Ptr makeCondition(std::shared_ptr<const query::QsRestrictor> const restr,
                                   RestrictorEntry const& restrictorEntry) {
    Restriction r(*restr);
    return r.generate(restrictorEntry);
}


/**
 * @brief Determine if the given ValueExpr represents a func that is one of the scisql functions that
 *        starts with `scisql_s2PtIn` that represents an area restrictor.
 *
 * This is a helper function for handleScisqlRestrictors.
 *
 * @param valueExpr The ValueExpr to check.
 * @return true if the ValueExpr is a function and starts with `scisql_s2PtIn` else false.
 */
bool isScisqlAreaFunc(query::ValueExpr const& valueExpr) {
    if (not valueExpr.isFunction()) return false;
    auto&& funcExpr = valueExpr.getFunction();
    if (funcExpr->getName().find("scisql_s2Pt") != 0) return false;
    return true;
}


/**
 * @brief If there is exactly one scisql area restrictor in the top level AND of the where clause,
 *        return it.
 *
 * @param whereClause The WHERE clause to look in.
 * @return std::shared_ptr<const query::FuncExpr> The scisql area restrictor functio FuncExpr if there
 *         was exactly one, else nullptr.
 */
std::shared_ptr<const query::FuncExpr> extractSingleScisqlAreaFunc(query::WhereClause const& whereClause) {
    auto topLevelAnd = whereClause.getRootAndTerm();
    std::shared_ptr<const query::FuncExpr> scisqlFunc;
    if (nullptr == topLevelAnd) return nullptr;
    for (auto const& boolTerm : topLevelAnd->_terms) {
        auto boolFactor = std::dynamic_pointer_cast<const query::BoolFactor>(boolTerm);
        if (nullptr == boolFactor) continue;
        for (auto boolFactorTerm : boolFactor->_terms) {
            auto compPredicate = std::dynamic_pointer_cast<const query::CompPredicate>(boolFactorTerm);
            if (nullptr == compPredicate) continue;
            if (compPredicate->op != query::CompPredicate::EQUALS_OP) continue;
            for (auto const& valueExpr: {compPredicate->left, compPredicate->right})
            if (isScisqlAreaFunc(*valueExpr)) {
                if (scisqlFunc != nullptr) {
                    return nullptr;
                }
                scisqlFunc = valueExpr->getFunction();
            }
        }
    }
    return scisqlFunc;
}


/**
 * @brief Add scisql restrictors for each QsRestrictor.
 *
 * This handles the case where a qserv areaspec function was passed into the WHERE clause by the user,
 * it adds scisql restrictor functions corresponding to the qserv area restrictor that is applied as a
 * result of the areaspec function.
 *
 * @param restrictors The qserv area restrictors to add scisql area restrictors for.
 * @param whereClause The query's WHERE clause, to which scisql area restrictors will be added.
 * @param fromList The query's FROM list, it is consulted to find the chunked tables.
 * @param context The query context.
 */
void addScisqlRestrictors(std::vector<std::shared_ptr<query::QsRestrictor>> const& restrictors,
                           query::FromList const& fromList,
                           query::WhereClause& whereClause,
                           query::QueryContext& context) {
    if (restrictors.empty()) return;

    auto const& tableList = fromList.getTableRefList();
    RestrictoryEntryList chunkedTables;
    getTable gt(*context.css, chunkedTables);
    std::for_each(tableList.begin(), tableList.end(), gt);
    // chunkedTables is now populated with a RestrictorEntry for each table in the FROM list that is chunked.
    if (chunkedTables.empty()) {
        throw qana::AnalysisError("Spatial restrictor without partitioned table.");
    }

    auto newTerm = std::make_shared<query::AndTerm>();
    // Add scisql spatial restrictions: for each of the qserv restrictors, generate a scisql restrictor
    // condition for each chunked table.
    for (auto const& qsRestrictor : restrictors) {
        for (auto const& chunkedTable : chunkedTables) {
            newTerm->_terms.push_back(makeCondition(qsRestrictor, chunkedTable));
        }
    }
    LOGS(_log, LOG_LVL_TRACE, "for restrictors: " << util::printable(restrictors) <<
                              " adding: " << newTerm);
    whereClause.prependAndTerm(newTerm);
}


/**
 * @brief Make a vector of ColumnRef derived from the given ValueExpr.
 */
query::ColumnRef::Vector resolveAsColumnRef(query::ValueExprPtr vexpr) {
    query::ColumnRef::Vector columnRefs;
    auto columnRef = vexpr->copyAsColumnRef();
    if (nullptr != columnRef)
        columnRefs.push_back(columnRef);
    return columnRefs;
}


/**
 * @brief Find out if the given ColumnRef represents a valid secondary index column.
 */
bool
lookupSecIndex(query::QueryContext& context,
               std::shared_ptr<query::ColumnRef> cr) {
    // Match cr as a column ref against the secondary index column for a
    // database's partitioning strategy.
    if ((!cr) || !context.css) { return false; }
    if (!context.css->containsDb(cr->getDb())
       || !context.css->containsTable(cr->getDb(), cr->getTable())) {
        throw qana::AnalysisError("Invalid db/table:" + cr->getDb() + "." + cr->getTable());
    }
    if (cr->getColumn().empty()) {
        return false;
    }
    std::vector<std::string> sics = context.css->getPartTableParams(
        cr->getDb(), cr->getTable()).secIndexColNames();
    return std::find(sics.begin(), sics.end(), cr->getColumn()) != sics.end();
}


/**  Create a QsRestrictor from the column ref and the set of specified values or NULL if one of the values is a non-literal.
 *
 *   @param restrictorType: The type of restrictor, only secondary index restrictors are handled
 *   @param context:        Context, used to get database schema informations
 *   @param cr:             Represent the column on which secondary index will be queried
 *   @return:               A Qserv restrictor or NULL if at least one element in values is a non-literal.
 */
query::QsRestrictor::Ptr newRestrictor(
    RestrictorType restrictorType,
    query::QueryContext const& context,
    std::shared_ptr<query::ColumnRef> cr,
    query::ValueExprPtrVector const& values)
{
    // Extract the literals, bailing out if we see a non-literal
    bool isValid = true;
    std::for_each(values.begin(), values.end(),
        [&](query::ValueExprPtr p) { isValid = isValid && p != nullptr && not p->copyAsLiteral().empty(); });
    if (!isValid) {
        return query::QsRestrictor::Ptr();
    }

    // Build the QsRestrictor
    query::QsRestrictor::Ptr restrictor = std::make_shared<query::QsRestrictor>();
    if (restrictorType==SECONDARY_INDEX_IN) {
        restrictor->_name = "sIndex";
    }
    if (restrictorType==SECONDARY_INDEX_NOT_IN) {
        restrictor->_name = "sIndexNotIn";
    }
    else if (restrictorType==SECONDARY_INDEX_BETWEEN) {
        restrictor->_name = "sIndexBetween";
    }
    else if (restrictorType==SECONDARY_INDEX_NOT_BETWEEN) {
        restrictor->_name = "sIndexNotBetween";
    }
    // sIndex and sIndexBetween have parameters as follows:
    // db, table, column, val1, val2, ...

    css::PartTableParams const partParam = context.css->getPartTableParams(cr->getDb(), cr->getTable());
    // Get the director column name
    std::string dirCol = partParam.dirColName;
    if (cr->getColumn() == dirCol) {
        // cr may be a column in a child table, in which case we must figure
        // out the corresponding column in the child's director to properly
        // generate a secondary index constraint.
        std::string dirDb = partParam.dirDb;
        std::string dirTable = partParam.dirTable;
        if (dirTable.empty()) {
            dirTable = cr->getTable();
            if (!dirDb.empty() && dirDb != cr->getDb()) {
                LOGS(_log, LOG_LVL_ERROR, "dirTable missing, but dirDb is set inconsistently for "
                     << cr->getDb() << "." << cr->getTable());
                return query::QsRestrictor::Ptr();
            }
            dirDb = cr->getDb();
        } else if (dirDb.empty()) {
            dirDb = cr->getDb();
        }
        if (dirDb != cr->getDb() || dirTable != cr->getTable()) {
            // Lookup the name of the director column in the director table
            dirCol = context.css->getPartTableParams(dirDb, dirTable).dirColName;
            if (dirCol.empty()) {
                LOGS(_log, LOG_LVL_ERROR, "dirCol missing for " << dirDb << "." << dirTable);
                return query::QsRestrictor::Ptr();
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "Restrictor dirDb " << dirDb << ", dirTable " << dirTable
             << ", dirCol " << dirCol << " as sIndex for " << cr->getDb() << "." << cr->getTable()
             << "." << cr->getColumn());
        restrictor->_params.push_back(dirDb);
        restrictor->_params.push_back(dirTable);
        restrictor->_params.push_back(dirCol);
    } else {
        LOGS_DEBUG("Restrictor " << cr->getDb() << "." << cr->getTable() <<  "." << cr->getColumn()
                   << " as sIndex");
        restrictor->_params.push_back(cr->getDb());
        restrictor->_params.push_back(cr->getTable());
        restrictor->_params.push_back(cr->getColumn());
    }

    std::transform(values.begin(), values.end(), std::back_inserter(restrictor->_params),
        [](query::ValueExprPtr p) -> std::string { return p->copyAsLiteral(); });
    return restrictor;
}


/**  Create QSRestrictors which will use secondary index
 *
 *   @param context:  Context used to analyze SQL query, allow to compute
 *                    column names and find if they are in secondary index.
 *   @param andTerm:  Intermediatre representation of a subset of a SQL WHERE clause
 *
 *   @return:         Qserv restrictors list
 */
query::QsRestrictor::PtrVector getSecIndexRestrictors(query::QueryContext& context,
                                                      query::AndTerm::Ptr andTerm) {
    query::QsRestrictor::PtrVector result;
    if (not andTerm) return result;

    for (auto&& term : andTerm->_terms) {
        auto factor = std::dynamic_pointer_cast<query::BoolFactor>(term);
        if (!factor) continue;
        for (auto factorTerm : factor->_terms) {
            query::ColumnRef::Vector columnRefs;
            query::QsRestrictor::Ptr restrictor;

            // Remark: computation below could be placed in a virtual *Predicate::newRestrictor() method
            // but this is not obvious because it would move restrictor-related code outside of
            // the current plugin.
            // IN predicate
            LOGS(_log, LOG_LVL_TRACE, "Check for SECONDARY_INDEX_IN restrictor");
            if (auto const inPredicate = std::dynamic_pointer_cast<query::InPredicate>(factorTerm)) {
                columnRefs = resolveAsColumnRef(inPredicate->value);
                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        auto restrictorType = inPredicate->hasNot ? SECONDARY_INDEX_NOT_IN : SECONDARY_INDEX_IN;
                        restrictor = newRestrictor(restrictorType, context, column_ref, inPredicate->cands);
                        LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_IN restrictor: " << *restrictor);
                        break; // Only want one per column.
                    }
                }
            } else if (auto const compPredicate =
                       std::dynamic_pointer_cast<query::CompPredicate>(factorTerm)) {
                // '=' predicate
                query::ValueExprPtr literalValue;

                // If the left side doesn't match any columns, check the right side.
                columnRefs = resolveAsColumnRef(compPredicate->left);
                if (columnRefs.empty()) {
                    columnRefs = resolveAsColumnRef(compPredicate->right);
                    literalValue = compPredicate->left;
                } else {
                    literalValue = compPredicate->right;
                }

                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        query::ValueExprPtrVector cands(1, literalValue);
                        restrictor = newRestrictor(SECONDARY_INDEX_IN, context, column_ref , cands);
                        if (restrictor) {
                            LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_IN restrictor: "
                                    << *restrictor << " for '=' predicate");
                            break; // Only want one per column.
                        }
                    }
                }
            } else if (auto const betweenPredicate =
                       std::dynamic_pointer_cast<query::BetweenPredicate>(factorTerm)) {
                // BETWEEN predicate
                LOGS(_log, LOG_LVL_TRACE, "Check for SECONDARY_INDEX_BETWEEN restrictor");
                columnRefs = resolveAsColumnRef(betweenPredicate->value);
                for (query::ColumnRef::Ptr const& column_ref : columnRefs) {
                    if (lookupSecIndex(context, column_ref)) {
                        query::ValueExprPtrVector cands;
                        cands.push_back(betweenPredicate->minValue);
                        cands.push_back(betweenPredicate->maxValue);
                        auto restrictorType = betweenPredicate->hasNot ? SECONDARY_INDEX_NOT_BETWEEN : SECONDARY_INDEX_BETWEEN;
                        restrictor = newRestrictor(restrictorType, context, column_ref, cands);
                        if (restrictor) {
                            LOGS(_log, LOG_LVL_DEBUG, "Add SECONDARY_INDEX_BETWEEN restrictor: "
                                                      << *restrictor);
                            break; // Only want one per column.
                        }
                    }
                }
            }

            if (restrictor) {
                result.push_back(restrictor);
            }
        }
    }
    return result;
}


/**
 * @brief Looks in the WHERE clause for use of columns from chunked tables where chunk restrictions can
 *        be added, and adds qserv restrictor functions if any are found.
 *
 * @param whereClause The WHERE clause of the SELECT statement.
 * @param context The context to add the restrictor functions to.
 */
void handleSecondaryIndex(query::WhereClause& whereClause, query::QueryContext& context) {
    // Merge in the implicit (i.e. secondary index) restrictors
    query::AndTerm::Ptr originalAnd(whereClause.getRootAndTerm());
    query::QsRestrictor::PtrVector const& secIndexPreds = getSecIndexRestrictors(context, originalAnd);
    context.addRestrictors(secIndexPreds);
}


/**
 * @brief Looks for scisql area restrictors in the WHERE clause and if there is exactly one in the
 *        top-level AND it adds a qserv area restictor to the query context.
 *
 * This should not be called if there is already a qserv area restrictor in the WHERE clause.
 *
 * @param selectStmt The SELECT statement to process restrictors for.
 * @param context The query context to be updated.
 */
void handleScisqlRestrictors(query::SelectStmt& stmt, query::QueryContext& context) {
    auto& whereClause = stmt.getWhereClause();
    if (whereClause.hasRestrs()) {
        // It's possible we could support this but current specification says we should only add a qserv area
        // restrictor if there is not already one in the WHERE clause.
        throw qana::AnalysisBug("handleScisqlRestrictors should not be called if the WHERE clause already "
                                "contains restrictors.");
    }

    // Get the scisql functions from the top-level AND, if there is 0 or more than 1 then return.
    auto scisqlFunc = extractSingleScisqlAreaFunc(whereClause);

    // determine if the columns in the scisql function are chunked; if they are not then return.

    // translate the function to a qserv area restrictor and add it to the context.
}


} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qana {


////////////////////////////////////////////////////////////////////////
// QservRestrictorPlugin implementation
////////////////////////////////////////////////////////////////////////
void
QservRestrictorPlugin::applyLogical(query::SelectStmt& stmt,
                                    query::QueryContext& context) {

    // Idea: For each of the qserv restrictors in the WHERE clause,
    // rewrite in the context of whatever chunked tables exist in the
    // FROM list.

    if (!context.css) {
        throw AnalysisBug("Missing metadata in context.");
    }

    // If there's no where clause then there's no need to do any work here.
    if (!stmt.hasWhereClause()) { return; }

    // Prepare to patch the WHERE clause
    query::WhereClause& whereClause = stmt.getWhereClause();

    if (whereClause.hasRestrs()) {
        // get where clause restrictors:
        auto restrictors = whereClause.getRestrs();
        // add restrictors to context:
        if (restrictors != nullptr) {
            context.addRestrictors(*restrictors);
            whereClause.resetRestrs();
            // make scisql functions for restrictors:
            addScisqlRestrictors(*restrictors, stmt.getFromList(), whereClause, context);
        }
    } else {
        handleScisqlRestrictors(stmt, context);
        // get scisql restrictors
        // add restrictors to context
    }

    handleSecondaryIndex(whereClause, context);
}


void QservRestrictorPlugin::applyPhysical(QueryPlugin::Plan& p,
                                          query::QueryContext& context) {
    // Probably nothing is needed here...
}


}}} // namespace lsst::qserv::qana
