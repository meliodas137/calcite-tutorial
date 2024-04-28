package org.pathirage.calcite.tutorial.planner;


import com.google.common.io.Resources;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.rel.rules.*;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.*;
import org.apache.calcite.jdbc.Driver;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.volcano.AbstractConverter;
import java.util.List;

public class SimpleQueryPlanner {

  static final List<RelOptRule> BASE_RULES =
      ImmutableList.of(CoreRules.AGGREGATE_STAR_TABLE,
          CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
          CalciteSystemProperty.COMMUTE.value()
              ? CoreRules.JOIN_ASSOCIATE
              : CoreRules.PROJECT_MERGE,
          CoreRules.FILTER_SCAN,
          CoreRules.PROJECT_FILTER_TRANSPOSE,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
          CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
          CoreRules.AGGREGATE_CASE_TO_FILTER,
          CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          CoreRules.PROJECT_WINDOW_TRANSPOSE,
          CoreRules.MATCH,
          CoreRules.JOIN_COMMUTE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          CoreRules.SORT_PROJECT_TRANSPOSE,
          CoreRules.SORT_JOIN_TRANSPOSE,
          CoreRules.SORT_REMOVE_CONSTANT_KEYS,
          CoreRules.SORT_UNION_TRANSPOSE,
          CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
          CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS,
          CoreRules.SAMPLE_TO_FILTER,
          CoreRules.FILTER_SAMPLE_TRANSPOSE,
          CoreRules.FILTER_WINDOW_TRANSPOSE);

  static final List<RelOptRule> ABSTRACT_RULES =
      ImmutableList.of(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
          CoreRules.UNION_PULL_UP_CONSTANTS,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          PruneEmptyRules.EMPTY_TABLE_INSTANCE,
          CoreRules.UNION_MERGE,
          CoreRules.INTERSECT_MERGE,
          CoreRules.MINUS_MERGE,
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
          CoreRules.FILTER_MERGE,
          DateRangeRules.FILTER_INSTANCE,
          CoreRules.INTERSECT_TO_DISTINCT,
          CoreRules.MINUS_TO_DISTINCT);

  static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES =
      ImmutableList.of(CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH,
          AbstractConverter.ExpandConversionRule.INSTANCE,
          CoreRules.JOIN_COMMUTE,
          CoreRules.PROJECT_TO_SEMI_JOIN,
          CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN,
          CoreRules.JOIN_TO_SEMI_JOIN,
          CoreRules.AGGREGATE_REMOVE,
          CoreRules.UNION_TO_DISTINCT,
          CoreRules.PROJECT_REMOVE,
          CoreRules.PROJECT_AGGREGATE_MERGE,
          CoreRules.AGGREGATE_JOIN_TRANSPOSE,
          CoreRules.AGGREGATE_MERGE,
          CoreRules.AGGREGATE_PROJECT_MERGE,
          CoreRules.CALC_REMOVE,
          CoreRules.SORT_REMOVE);

  static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.CALC_REDUCE_EXPRESSIONS,
          CoreRules.WINDOW_REDUCE_EXPRESSIONS,
          CoreRules.JOIN_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_VALUES_MERGE,
          CoreRules.PROJECT_FILTER_VALUES_MERGE,
          CoreRules.PROJECT_VALUES_MERGE,
          CoreRules.AGGREGATE_VALUES);

  private final Planner planner;

  public SimpleQueryPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
            // case when they are read, and whether identifiers are matched case-sensitively.
            .setLex(Lex.MYSQL)
            .build())
        // Sets the schema to use by the planner
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        // Context provides a way to store data within the planner session that can be accessed in planner rules.
        .context(Contexts.EMPTY_CONTEXT)
        // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
        .ruleSets(RuleSets.ofList(RelOptRules.CALC_RULES))
        // Custom cost factory to use during optimization
        .costFactory(null)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
    // RelOptUtil.registerDefaultRules(this.planner, false, false);
  }

  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = planner.validate(sqlNode);

    return planner.rel(validatedSqlNode).project();
  }

  public static void main(String[] args) throws IOException, SQLException, ValidationException, RelConversionException {
    // Simple connection implementation for loading schema from sales.json
    CalciteConnection connection = new SimpleCalciteConnection();
    String salesSchema = Resources.toString(SimpleQueryPlanner.class.getResource("/sales.json"), Charset.defaultCharset());
    // ModelHandler reads the sales schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + salesSchema);

    // Create the query planner with sales schema. conneciton.getSchema returns default schema name specified in sales.json
    SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema()));
    RelNode loginalPlan = queryPlanner.getLogicalPlan("select product from orders");
    System.out.println(RelOptUtil.toString(loginalPlan));
  }
}
