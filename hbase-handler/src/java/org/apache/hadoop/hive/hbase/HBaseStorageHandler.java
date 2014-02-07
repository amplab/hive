/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe.ColumnMapping;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * HBaseStorageHandler provides a HiveStorageHandler implementation for
 * HBase.
 */
public class HBaseStorageHandler extends DefaultStorageHandler
  implements HiveMetaHook, HiveStoragePredicateHandler {

  final static public String DEFAULT_PREFIX = "default.";

  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);

  private Configuration hbaseConf;
  private HBaseAdmin admin;

  private HBaseAdmin getHBaseAdmin() throws MetaException {
    try {
      if (admin == null) {
        admin = new HBaseAdmin(hbaseConf);
      }
      return admin;
    } catch (IOException ioe) {
      throw new MetaException(StringUtils.stringifyException(ioe));
    }
  }

  private String getHBaseTableName(Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).
    String tableName = tbl.getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
        HBaseSerDe.HBASE_TABLE_NAME);
    }
    if (tableName == null) {
      tableName = tbl.getDbName() + "." + tbl.getTableName();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    return tableName;
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void commitDropTable(
    Table tbl, boolean deleteData) throws MetaException {

    try {
      String tableName = getHBaseTableName(tbl);
      boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
      if (deleteData && !isExternal) {
        if (getHBaseAdmin().isTableEnabled(tableName)) {
          getHBaseAdmin().disableTable(tableName);
        }
        getHBaseAdmin().deleteTable(tableName);
      }
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void preCreateTable(Table tbl) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

    // We'd like to move this to HiveMetaStore for any non-native table, but
    // first we need to support storing NULL for location on a table
    if (tbl.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for HBase.");
    }

    try {
      String tableName = getHBaseTableName(tbl);
      Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParam.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
      List<ColumnMapping> columnsMapping = null;

      columnsMapping = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping);

      HTableDescriptor tableDesc;

      if (!getHBaseAdmin().tableExists(tableName)) {
        // if it is not an external table then create one
        if (!isExternal) {
          // Create the column descriptors
          tableDesc = new HTableDescriptor(tableName);
          Set<String> uniqueColumnFamilies = new HashSet<String>();

          for (ColumnMapping colMap : columnsMapping) {
            if (!colMap.hbaseRowKey) {
              uniqueColumnFamilies.add(colMap.familyName);
            }
          }

          for (String columnFamily : uniqueColumnFamilies) {
            tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily)));
          }

          getHBaseAdmin().createTable(tableDesc);
        } else {
          // an external table
          throw new MetaException("HBase table " + tableName +
              " doesn't exist while the table is declared as an external table.");
        }

      } else {
        if (!isExternal) {
          throw new MetaException("Table " + tableName + " already exists"
            + " within HBase; use CREATE EXTERNAL TABLE instead to"
            + " register it in Hive.");
        }
        // make sure the schema mapping is right
        tableDesc = getHBaseAdmin().getTableDescriptor(Bytes.toBytes(tableName));

        for (int i = 0; i < columnsMapping.size(); i++) {
          ColumnMapping colMap = columnsMapping.get(i);

          if (colMap.hbaseRowKey) {
            continue;
          }

          if (!tableDesc.hasFamily(colMap.familyNameBytes)) {
            throw new MetaException("Column Family " + colMap.familyName
                + " is not defined in hbase table " + tableName);
          }
        }
      }

      // ensure the table is online
      new HTable(hbaseConf, tableDesc.getName());
    } catch (MasterNotRunningException mnre) {
      throw new MetaException(StringUtils.stringifyException(mnre));
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    } catch (SerDeException se) {
      throw new MetaException(StringUtils.stringifyException(se));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    String tableName = getHBaseTableName(table);
    try {
      if (!isExternal && getHBaseAdmin().tableExists(tableName)) {
        // we have created an HBase table, so we delete it to roll back;
        if (getHBaseAdmin().isTableEnabled(tableName)) {
          getHBaseAdmin().disableTable(tableName);
        }
        getHBaseAdmin().deleteTable(tableName);
      }
    } catch (IOException ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public Configuration getConf() {
    return hbaseConf;
  }

  @Override
  public void setConf(Configuration conf) {
    hbaseConf = HBaseConfiguration.create(conf);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveHBaseTableInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveHBaseTableOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return HBaseSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureInputJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {
      configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {
      configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();

    jobProperties.put(
      HBaseSerDe.HBASE_COLUMNS_MAPPING,
      tableProperties.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING));
    jobProperties.put(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,"string"));

    String tableName =
      tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName =
        tableProperties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);
  }

  /**
   *
   * @param predicate - Predicate to be analyzed
   * @param columns   - List of columns in the table
   * @param serde     - HBaseSerde corresponding to the table
   * @param colTypes  - List of column types
   * @return True if either all the predicates have either binary storage or contains an "=" as comparison operator
   *         has a string datatype , otherwise False. Since we can push "=" regardless of whether the column is binary
   *         or not and also binary-storage/string type columns with any comparison operator can be pushed. This method checks if
   *         predicate satisfies this condition.
   *
   */
  private boolean checkAllBinaryOrEquals(ExprNodeDesc predicate,List<String> columns,List<String> colTypes,HBaseSerDe serde){

    String comparisonOp,columnName;
    int colIndex;
    Stack stack = new Stack<ExprNodeDesc>();
    stack.push(predicate);

    while(!stack.isEmpty()){
      ExprNodeDesc top = (ExprNodeDesc)stack.pop();

      if((top instanceof ExprNodeGenericFuncDesc) && (top.getChildren().size() == 2)){

        ExprNodeDesc child1 = top.getChildren().get(0);
        ExprNodeDesc child2 = top.getChildren().get(1);
        if (((child1 instanceof ExprNodeColumnDesc)
            && (child2 instanceof ExprNodeConstantDesc)) || ((child2 instanceof ExprNodeColumnDesc)
                && (child1 instanceof ExprNodeConstantDesc)) ){

          if (((ExprNodeGenericFuncDesc)top).getGenericUDF() instanceof GenericUDFBridge) {
            GenericUDFBridge func = (GenericUDFBridge) ((ExprNodeGenericFuncDesc)top).getGenericUDF();
            comparisonOp = func.getUdfName();
          } else {
            comparisonOp = ((ExprNodeGenericFuncDesc)top).getGenericUDF().getClass().getName();
          }
          columnName = (child1 instanceof ExprNodeColumnDesc)?((ExprNodeColumnDesc)child1).getColumn():((ExprNodeColumnDesc)child2).getColumn();
          colIndex = columns.indexOf(columnName);

          if(columnName.equals(columns.get(serde.getKeyColumnOffset())) || !(colTypes.get(colIndex).equalsIgnoreCase("string") || comparisonOp.equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual") || serde.getStorageFormatOfCol(colIndex).get(0))){
            return false;
          }
        }
        else{
          stack.insertElementAt(child1,stack.size());
          stack.insertElementAt(child2,stack.size());
        }
      }
    }
    return true;
  }

  /**
   *
   * @param predicate - predicate to be analyzed
   * @param columns   - List of columns as in serde
   * @param serde     - serde for the input table
   * @return          - List of size 2, with index 0 containing residual predicate remaining after processing
   *                    this predicate and index 1 containing predicate that can be pushed. Returns null, if
   *                    input predicate is null.
   */

  List<ExprNodeDesc> checkNonKeyPredicates(ExprNodeDesc predicate,List<String> columns,List<String> colTypes,HBaseSerDe serde){

    List retPred = new ArrayList<ExprNodeDesc>(2);
    if(predicate == null) {
      return null;
    }
    if(checkAllBinaryOrEquals(predicate, columns,colTypes,serde)){
        retPred.add(null);
        retPred.add(predicate);
        return retPred;
    }
    else{
      if(!FunctionRegistry.isOpAnd(predicate)){
        ExprNodeDesc child1 = predicate.getChildren().get(0);
        ExprNodeDesc child2 = predicate.getChildren().get(1);

        if (((child1 instanceof ExprNodeColumnDesc)
            && !(((ExprNodeColumnDesc)child1).getColumn()).equals(columns.get(serde.getKeyColumnOffset())) &&(child2 instanceof ExprNodeConstantDesc)) || ((child2 instanceof ExprNodeColumnDesc)
            && !(((ExprNodeColumnDesc)child2).getColumn()).equals(columns.get(serde.getKeyColumnOffset())) &&(child1 instanceof ExprNodeConstantDesc))){
          retPred.add(null);
          retPred.add(predicate);
        }
        else{
          retPred.add(predicate);
          retPred.add(null);
        }
        return retPred;
      }
      else{
        ExprNodeDesc child1 = predicate.getChildren().get(0);
        ExprNodeDesc child2 = predicate.getChildren().get(1);

        List<ExprNodeDesc> retChild1 = checkNonKeyPredicates(child1, columns,colTypes,serde);
        List<ExprNodeDesc> retChild2 = checkNonKeyPredicates(child2, columns,colTypes,serde);

        if((retChild1 == null) && (retChild2!=null)) {
          return retChild2;
        }
        else if ((retChild2 == null) && (retChild1!= null)){
          return retChild1;
        }
        else{
          if(retChild1.get(0)==null && retChild2.get(0)!=null){
            retPred.add(retChild2.get(0));
          }
          else if(retChild1.get(0)!=null && retChild2.get(0)==null){
            retPred.add(retChild1.get(0));
          }
          else if(retChild1.get(0)==null && retChild2.get(0)==null){
            retPred.add(null);
          }
          else{
            List<ExprNodeDesc> temp = new ArrayList<ExprNodeDesc>();
            temp.add(retChild1.get(0));
            temp.add(retChild2.get(0));
            retPred.add(new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,FunctionRegistry.getGenericUDFForAnd(),temp));
          }

          if(retChild1.get(1)==null && retChild2.get(1)!=null){
            retPred.add(retChild2.get(1));
          }
          else if(retChild1.get(1)!=null && retChild2.get(1)==null){
            retPred.add(retChild1.get(1));
          }
          else if(retChild1.get(1)==null && retChild2.get(1)==null){
            retPred.add(null);
          }
          else{
            List<ExprNodeDesc> temp = new ArrayList<ExprNodeDesc>();
            temp.add(retChild1.get(1));
            temp.add(retChild2.get(1));
            retPred.add(new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,FunctionRegistry.getGenericUDFForAnd(),temp));
          }
          return retPred;
        }

      }

    }
  }


  @Override
  public DecomposedPredicate decomposePredicate(
    JobConf jobConf,
    Deserializer deserializer,
    ExprNodeDesc predicate)
  {
    String columnNameProperty = jobConf.get(
      org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);
    List<String> columnNames =
      Arrays.asList(columnNameProperty.split(","));
    List<String> colTypes =
      Arrays.asList(jobConf.get(org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES).split(","));

    HBaseSerDe hbaseSerde = (HBaseSerDe) deserializer;
    int keyColPos = hbaseSerde.getKeyColumnOffset();
    String keyColType = colTypes.get(keyColPos);
    IndexPredicateAnalyzer analyzer =
      HiveHBaseTableInputFormat.newIndexPredicateAnalyzer(columnNames.get(keyColPos), keyColType,
        hbaseSerde.getStorageFormatOfCol(keyColPos).get(0));
    List<IndexSearchCondition> searchConditions =
      new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualKeyPredicate =
      analyzer.analyzePredicate(predicate, searchConditions);
    ExprNodeDesc pushedKeyPredicate;

    int scSize = searchConditions.size();
    if (scSize < 1 || 2 < scSize) {
      // Either there was nothing which could be pushed down (size = 0),
      // there were complex predicates which we don't support yet.
      // Currently supported are one of the form:
      // 1. key < 20                        (size = 1)
      // 2. key = 20                        (size = 1)
      // 3. key < 20 and key > 10           (size = 2)
      pushedKeyPredicate = null;
      residualKeyPredicate = predicate;
    }
    else if (scSize == 2 &&
        (searchConditions.get(0).getComparisonOp()
        .equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual") ||
        searchConditions.get(1).getComparisonOp()
        .equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual"))) {
      // If one of the predicates is =, then any other predicate with it is illegal.
      pushedKeyPredicate = null;
      residualKeyPredicate = predicate;
    }
    else{
      pushedKeyPredicate = analyzer.translateSearchConditions(searchConditions);
    }

    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = null;

    // checking for non-key predicates that can be pushed in the residual
    // predicate after processing for key column
    List<ExprNodeDesc> analyzedPredicates = checkNonKeyPredicates(residualKeyPredicate, columnNames,colTypes,hbaseSerde);

    if(analyzedPredicates != null){

      if(pushedKeyPredicate == null && analyzedPredicates.get(1) != null){
        decomposedPredicate.pushedPredicate = analyzedPredicates.get(1);
      }
      else if(pushedKeyPredicate != null && analyzedPredicates.get(1) == null){
        decomposedPredicate.pushedPredicate = pushedKeyPredicate;
      }
      else if(pushedKeyPredicate != null && analyzedPredicates.get(1) != null){
        List<ExprNodeDesc> temp = new ArrayList<ExprNodeDesc>();
        temp.add(pushedKeyPredicate);
        temp.add(analyzedPredicates.get(1));
        decomposedPredicate.pushedPredicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,FunctionRegistry.getGenericUDFForAnd(),temp);
      }
      decomposedPredicate.residualPredicate = analyzedPredicates.get(0);
    }
    else{
      decomposedPredicate.pushedPredicate = pushedKeyPredicate;
      decomposedPredicate.residualPredicate = residualKeyPredicate;
    }
    return decomposedPredicate;
  }
}
