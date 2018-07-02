package org.nuxeo.sample;

import static org.junit.Assert.*;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.test.DefaultRepositoryInit;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.ecm.core.test.CoreFeature;
import org.nuxeo.ecm.core.query.QueryParseException;
import org.nuxeo.ecm.core.query.sql.model.DateLiteral;
import org.nuxeo.ecm.core.query.sql.model.DoubleLiteral;
import org.nuxeo.ecm.core.query.sql.model.EsHint;
import org.nuxeo.ecm.core.query.sql.model.EsIdentifierList;
import org.nuxeo.ecm.core.query.sql.model.Expression;
import org.nuxeo.ecm.core.query.sql.model.FromClause;
import org.nuxeo.ecm.core.query.sql.model.Function;
import org.nuxeo.ecm.core.query.sql.model.IntegerLiteral;
import org.nuxeo.ecm.core.query.sql.model.LiteralList;
import org.nuxeo.ecm.core.query.sql.model.Operand;
import org.nuxeo.ecm.core.query.sql.model.OperandList;
import org.nuxeo.ecm.core.query.sql.model.Operator;
import org.nuxeo.ecm.core.query.sql.model.OrderByClause;
import org.nuxeo.ecm.core.query.sql.model.OrderByList;
import org.nuxeo.ecm.core.query.sql.model.Predicate;
import org.nuxeo.ecm.core.query.sql.model.Reference;
import org.nuxeo.ecm.core.query.sql.model.SQLQuery;
import org.nuxeo.ecm.core.query.sql.model.SelectClause;
import org.nuxeo.ecm.core.query.sql.model.SelectList;
import org.nuxeo.ecm.core.query.sql.model.StringLiteral;
import org.nuxeo.ecm.core.query.sql.model.WhereClause;
import org.nuxeo.ecm.core.query.sql.SQLQueryParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @see <a href="https://doc.nuxeo.com/corg/unit-testing/">Unit Testing</a>
 */
@RunWith(FeaturesRunner.class)
@Features(CoreFeature.class)
@RepositoryConfig(init = DefaultRepositoryInit.class, cleanup = Granularity.METHOD)
@Deploy("org.nuxeo.sample.test-queries-core")
public class TestWhereClauses {

    private static final Log log = LogFactory.getLog(TestWhereClauses.class);

    @Inject
    protected CoreSession session;

    @Test
    public void emptyTest() {
        assertNull(null);
    }

    /**
     * Tests the manual query creation and parser by comparing the two queries (the manual one ith the parsed one).
     *
     * <pre>
     *              OR
     *    title="test"         AND
     *              p2>=10.2            &lt;
     *                          p1+p2        5
     * </pre>
     */
    @Test
    public void testWhereClause() {
        SQLQuery query = SQLQueryParser.parse("SELECT p1, p2 FROM t WHERE title = \"test\" OR p2 >= 10.2 AND p1 + p2 < 5");

        Expression expr1 = new Expression(new Reference("p1"), Operator.SUM, new Reference("p2"));
        expr1 = new Expression(expr1, Operator.LT, new IntegerLiteral(5));

        Predicate expr2 = new Predicate(new Reference("p2"), Operator.GTEQ, new DoubleLiteral(10.2));
        expr1 = new Expression(expr2, Operator.AND, expr1);

        expr2 = new Predicate(new Reference("title"), Operator.EQ, new StringLiteral("test"));

        Predicate pred = new Predicate(expr2, Operator.OR, expr1);

        // create the query by hand
        SQLQuery myquery = new SQLQuery(new SelectClause(), new FromClause(), new WhereClause(pred));
        myquery.getSelectClause().add(new Reference("p1"));
        myquery.getSelectClause().add(new Reference("p2"));
        myquery.getFromClause().add("t");

        assertEquals(myquery, query);
    }

    /**
     * Same query as before but with parenthesis on first OR condition.
     *
     * <pre>
     *                          AND
     *               OR                     <
     *                                p1+p2      5
     *    title="test"   p2>=10.2
     * </pre>
     */
    @Test
    public void testWhereClauseWithParenthesis() {
        SQLQuery parsedQuery = SQLQueryParser.parse("SELECT p1, p2 FROM t WHERE (title = \"test\" OR p2 >= 10.2) AND p1 + p2 < 5");

        // build the query by hand
        Predicate expr1 = new Predicate(new Reference("title"), Operator.EQ, new StringLiteral("test"));
        Expression expr2 = new Expression(new Reference("p2"), Operator.GTEQ, new DoubleLiteral(10.2));
        expr1 = new Predicate(expr1, Operator.OR, expr2);

        expr2 = new Expression(new Reference("p1"), Operator.SUM, new Reference("p2"));
        expr2 = new Expression(expr2, Operator.LT, new IntegerLiteral(5));

        Predicate pred = new Predicate(expr1, Operator.AND, expr2);

        SQLQuery builtQuery = new SQLQuery(new SelectClause(), new FromClause(), new WhereClause(pred));

        builtQuery.getSelectClause().add(new Reference("p1"));
        builtQuery.getSelectClause().add(new Reference("p2"));
        builtQuery.getFromClause().add("t");

        log.debug("NOTE THAT parsedQuery AND builtQuery DO NOT PRINT THE SAME WAY");
        log.debug("parsedQuery: " + parsedQuery);
        log.debug("builtQuery: " + builtQuery);

        // even though these two queries do not print the same in a debug statement, they are in fact equal
        assertEquals(parsedQuery, builtQuery);
    }


    @Test
    public void testTryToParse() {

        // q1 will fail to parse -- the IN operator requires a list which is defined using parentheses
        try {
            SQLQuery q1 = SQLQueryParser.parse("SELECT * FROM Document WHERE ecm:primaryType IN 'File'");
            log.debug(q1); 
        } catch (org.nuxeo.ecm.core.query.QueryParseException e) {
            log.error("Can't parse q1");
        }

        // q2 will parse correctly
        try {
            SQLQuery q2 = SQLQueryParser.parse("SELECT * FROM Document WHERE ecm:primaryType IN ('File')");
            log.debug(q2); 
        } catch (org.nuxeo.ecm.core.query.QueryParseException e) {
            log.error("Can't parse q2");
        }

    }

}
