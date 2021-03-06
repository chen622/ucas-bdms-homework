/*
 * MIT License
 *
 * Copyright (c) 2021 Chenming C
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import java.io.*;
import java.net.URI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class is used to implement the distinct function.
 */
public class Hw1Grp4 {

  /**
   * This method is the entrance of this program.
   * @param args R=file-name select:R1,gt,5.1 distinct:R2,R3,R5
   */
  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("java HW1 R=<file> select:R<1>,<gt>,<5.1> distinct:<R2,R3,R5>");
      System.exit(1);
    }

    Logger.getRootLogger().setLevel(Level.ERROR);

    // get filename from args
    String fileName = args[0].substring(2);

    // get compare expression from args
    String[] compareExp = args[1].substring(7).split(",");
    int compareIndex = Integer.parseInt(compareExp[0].substring(1));
    String compareType = compareExp[1]; // compare operator
    double compareValue = Double.parseDouble(compareExp[2]);

    // get distinct columns from args
    String[] distinct = args[2].substring(9).split(",");
    int[] distinctIndex = new int[distinct.length];
    for (int i = 0; i < distinct.length; i++) {
      distinctIndex[i] = Integer.parseInt(distinct[i].substring(1));
    }

    // read from hdfs
    HashMap<String, String[]> tableData = new HashMap<>();
    String filePath = "hdfs://localhost:9000" + fileName;
    try (FileSystem fs = FileSystem.get(URI.create(filePath), new Configuration());
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
    ) {
      String s;
      while ((s = in.readLine()) != null) {
        String[] cols = s.split("\\|");
        String[] result = new String[distinctIndex.length];
        // check the row has meet the condition or not
        if (compare(compareType, compareValue, Double.parseDouble(cols[compareIndex]))) {
          for (int i = 0; i < distinctIndex.length; i++) {
            result[i] = cols[distinctIndex[i]];
          }
          // put into the hash map
          tableData.put(String.join(",", result), result);
        }
      }

    } catch (IOException exception) {
      System.out.println("open file error: " + exception.getMessage());
      System.exit(-1);
    } catch (IllegalArgumentException exception) {
      System.out.println("file is not exist: " + exception.getMessage());
      System.exit(-2);
    }

    // write into hbase
    String tableName = "Result";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    String colName = "res";
    HColumnDescriptor cf = new HColumnDescriptor(colName);
    htd.addFamily(cf);
    Configuration configuration = HBaseConfiguration.create();
    try (HBaseAdmin hAdmin = new HBaseAdmin(configuration)) {
      if (hAdmin.tableExists(tableName)) {
        hAdmin.disableTable(tableName);
        hAdmin.deleteTable(tableName);
      }
      hAdmin.createTable(htd);

      HTable table = new HTable(configuration, tableName);
      List<Put> puts = new ArrayList<>();

      int i = 0;
      for (String[] rowData : tableData.values()) {
        Put put = new Put((i++ + "").getBytes());
        for (int j = 0; j < distinct.length; j++) {
          put.add(colName.getBytes(), distinct[j].getBytes(), rowData[j].getBytes());
        }
        puts.add(put);
      }
      table.put(puts);
      System.out.println("write hbase success");
      table.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("write hbase error: " + e.getMessage());
      System.exit(-3);
    }
  }

  /**
   * This method is used to check whether the row meets the comparison condition.
   * @param compareType  The compare operator
   * @param compareValue The condition of the comparison
   * @param value        The value of the row
   * @return The row has meet the comparison condition or not
   */
  static boolean compare(String compareType, double compareValue, double value) {
    switch (compareType) {
      case "gt":
        return value > compareValue;
      case "ge":
        return value >= compareValue;
      case "eq":
        return value == compareValue;
      case "ne":
        return value != compareValue;
      case "le":
        return value <= compareValue;
      case "lt":
        return value < compareValue;
      default:
        return false;
    }
  }

}
