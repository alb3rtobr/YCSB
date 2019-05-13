/**
 * 
 */
package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
/**
 * 
 * @author alb3rtobr
 *
 */
public class GeodeWorkload extends CoreWorkload {
/**
 * 
 */
  public static final String SERVERFUNCTION_PROPORTION_PROPERTY = "functionproportion";
  public static final String SERVERFUNCTION_PROPORTION_PROPERTY_DEFAULT = "0.0";
  public static final String SERVERFUNCTION_PROPERTY = "functionname";
  public static final String SERVERFUNCTION_PROPERTY_DEFAULT="";

  private String functionName;

  @Override
  public void init(Properties p) throws WorkloadException{
    super.init(p);
    functionName = p.getProperty(SERVERFUNCTION_PROPERTY, SERVERFUNCTION_PROPERTY_DEFAULT);
    operationchooser = createOperationGenerator(p);
  }


  // Not @Override but method hidding
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double readproportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    final double updateproportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    final double insertproportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    final double scanproportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    final double functionproportion = Double.parseDouble(p.getProperty(
        SERVERFUNCTION_PROPORTION_PROPERTY, SERVERFUNCTION_PROPORTION_PROPERTY_DEFAULT));

    final DiscreteGenerator operationchooser = new DiscreteGenerator();
    if (readproportion > 0) {
      operationchooser.addValue(readproportion, "READ");
    }

    if (updateproportion > 0) {
      operationchooser.addValue(updateproportion, "UPDATE");
    }

    if (insertproportion > 0) {
      operationchooser.addValue(insertproportion, "INSERT");
    }

    if (scanproportion > 0) {
      operationchooser.addValue(scanproportion, "SCAN");
    }

    if (readmodifywriteproportion > 0) {
      operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
    }

    if (functionproportion > 0) {
      operationchooser.addValue(functionproportion, "SERVERFUNCTION");
    }
    return operationchooser;
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    case "SERVERFUNCTION":
      doTransactionServerFunction(db);
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }


  public void doTransactionServerFunction(DB db) {
    Properties functionProps=new Properties();
    functionProps.put("regionName", table);
    functionProps.put("functionName", functionName);
    db.executeServerFunction(functionProps);
  }

}
