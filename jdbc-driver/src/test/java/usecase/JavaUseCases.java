package usecase;

import java.util.List;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Date;

import acolyte.jdbc.CompositeHandler;
import acolyte.jdbc.StatementHandler;
import acolyte.jdbc.UpdateResult;
import acolyte.jdbc.QueryResult;

import acolyte.jdbc.StatementHandler.Parameter;

import static acolyte.jdbc.RowLists.booleanList;
import static acolyte.jdbc.RowLists.rowList1;
import static acolyte.jdbc.RowLists.rowList3;
import static acolyte.jdbc.RowList.Column;

/**
 * Use cases for testing.
 * 
 * @author Cedric Chantepie
 */
public final class JavaUseCases {
    // Configure in anyway JDBC with following url,
    // declaring handler registered with 'my-handler-id' will be used.
    protected static final String jdbcUrl = 
        "jdbc:acolyte:anything-you-want?handler=my-handler-id";

    /**
     * Use case #1 - Quick start
     */
    public static Connection useCase1() throws SQLException {
        // Prepare handler
        final StatementHandler handler = new CompositeHandler().
            withQueryDetection("^SELECT ", // regex test from beginning
                               "EXEC that_proc"). // second detection regex
            withUpdateHandler(new CompositeHandler.UpdateHandler() {
                    // Handle execution of update statement (not query)
                    public UpdateResult apply(String sql, 
                                              List<Parameter> parameter) {

                        if (sql.startsWith("DELETE ")) {
                            // Process deletion ...

                            return /* deleted = */new UpdateResult(2);
                        }

                        // ... Process ...

                        return /* count = */UpdateResult.One;
                    }
                }).
            withQueryHandler(new CompositeHandler.QueryHandler () {
                    public QueryResult apply(String sql, 
                                             List<Parameter> params) {

                        if (sql.startsWith("SELECT ")) {
                            return rowList1(String.class).asResult();
                        }

                        // ... EXEC that_proc (see previous withQueryDetection)

                        // Prepare list of 2 rows
                        // with 3 columns of types String, Float, Date
                        return rowList3(String.class, Float.class, Date.class).
                            withLabel(1, "String"). // Optional: set labels
                            withLabel(3, "Date"). 
                            append("str", 1.2f, new Date(1l)).
                            append("val", 2.34f, null).
                            asResult();
                    }
                });

        // Register prepared handler with expected ID 'my-handler-id'
        acolyte.jdbc.Driver.register("my-handler-id", handler);

        // ... then connection is managed through |handler|
        return DriverManager.getConnection(jdbcUrl);
    } // end of useCase1

    /**
     * Use case #2 - Column definitions
     */
    public static Connection useCase2() throws SQLException {
        final StatementHandler handler = new CompositeHandler().
            withQueryDetection("^SELECT ").
            withQueryHandler(new CompositeHandler.QueryHandler() {
                    public QueryResult apply(String sql, 
                                             List<Parameter> params) {

                        // Prepare list of 2 rows
                        // with 3 columns of types String, Float, Date
                        return rowList3(Column(String.class, "str"),
                                        Column(Float.class, "f"), 
                                        Column(Date.class, "date")).
                            append("text", 2.3f, new Date(3l)).
                            append("label", 4.56f, new Date(4l)).
                            asResult();
                    }
                });

        // Register prepared handler with expected ID 'my-handler-id'
        acolyte.jdbc.Driver.register("my-handler-id", handler);

        // ... then connection is managed through |handler|
        return DriverManager.getConnection(jdbcUrl);
    } // end of useCase2

    /**
     * Use case #3 - Warnings
     */
    public static Connection useCase3() throws SQLException {
        final StatementHandler handler = new CompositeHandler().
            withQueryDetection("^SELECT ").
            withQueryDetection("^EXEC ").
            withQueryHandler(new CompositeHandler.QueryHandler() {
                    public QueryResult apply(String sql, 
                                             List<Parameter> params) {

                        if (sql.startsWith("EXEC ")) {
                            return QueryResult.
                                Nil.withWarning(new SQLWarning("Warn EXEC"));

                        } // end of if

                        return QueryResult.Nil;
                    }
                }).
            withUpdateHandler(new CompositeHandler.UpdateHandler() {
                    // Handle execution of update statement (not query)
                    public UpdateResult apply(String sql, 
                                              List<Parameter> parameter) {

                        if (sql.startsWith("DELETE ")) {
                            return UpdateResult.
                                Nothing.withWarning(new SQLWarning("Warn DELETE"));

                        } // end of if

                        return UpdateResult.Nothing;
                    }
                });

        // Register prepared handler with expected ID 'my-handler-id'
        acolyte.jdbc.Driver.register("my-handler-id", handler);

        // ... then connection is managed through |handler|
        return DriverManager.getConnection(jdbcUrl);
    } // end of useCase3

    /**
     * Use case #4 - Row list convinience constructor.
     */
    public static Connection useCase4() throws SQLException {
        return acolyte.jdbc.Driver.
            connection(new CompositeHandler().
                       withQueryDetection("^SELECT ").
                       withQueryHandler(new CompositeHandler.QueryHandler() {
                               public QueryResult apply(String sql, 
                                                        List<Parameter> ps) {
                                   
                                   return booleanList().append(true).asResult();
                               }
                           }));

        
    } // end of useCase4

    /**
     * Use case #5 - Generated keys
     */
    public static Connection useCase5() throws SQLException {
        return acolyte.jdbc.Driver.
            connection(new CompositeHandler().
                       withUpdateHandler(new CompositeHandler.UpdateHandler() {
                               public UpdateResult apply(String sql, 
                                                         List<Parameter> ps) {

                                   // One update with generated keys
                                   return UpdateResult.One.
                                       withGeneratedKeys(acolyte.jdbc.RowLists.
                                                         intList().append(100));
                               }
                           }));

        
    } // end of useCase5
} // end of class JavaUseCases
