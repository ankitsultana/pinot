/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class CustomQueryParser {

  public static String getQueryFingerprint(String query) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(query).getSqlNode();
    FingerprintVisitor visitor = new FingerprintVisitor();
    SqlNode newNode = visitor.visit(sqlNode);
    String result = newNode.toSqlString(null, false).getSql();
    System.out.println("Table: " + visitor._queriedTables.toString());
    return result;
  }

  static class FingerprintVisitor extends SqlShuttle {
    Set<String> _queriedTables = new HashSet<>();

    @Override
    public SqlNode visit(SqlLiteral literal) {
      return SqlLiteral.createCharString("?", literal.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlCall call) {
      if (call instanceof SqlTableRef) {
        SqlTableRef tableRef = (SqlTableRef) call;
        _queriedTables.add(tableRef.getOperandList().get(0).toString());
      } else if (call instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) call;
        _queriedTables.add(select.getFrom().toString());
      }
      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
      if (nodeList.stream().allMatch(node -> node.getKind().equals(SqlKind.LITERAL))) {
        return new SqlNodeList(nodeList.getParserPosition());
      }
      return super.visit(nodeList);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      return super.visit(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
      return super.visit(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
      return super.visit(param);
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return super.visit(intervalQualifier);
    }

    public SqlNode visit(SqlNode sqlNode) {
      return sqlNode.accept(this);
    }
  }
}
