package com.muheda.notice.hbase;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * @Author: Sorin
 * @Descriptions: HBaseDao操作公共类
 * @Date: Created in 2018/3/22
 */
@Component("hBaseDaoUtil")
public class HBaseDaoUtil {

    Logger logger = Logger.getLogger(this.getClass());

    // 关闭连接
    public static void close() {
        if (HconnectionFactory.connection != null) {
            try {
                HconnectionFactory.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @Descripton: 创建表
     * @Author: Sorin
     * @param tableName
     * @param familyColumn
     * @Date: 2018/3/22
     */
    public void createTable(String tableName, String... familyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = HconnectionFactory.connection.getAdmin();) {
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String fc : familyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
            }
            admin.createTable(htd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Descripton: 删除表
     * @Author: Sorin
     * @param tableName
     * @Date: 2018/3/22
     */
    public void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = HconnectionFactory.connection.getAdmin();){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Descripton: 插入或者更新数据
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @Date: 2018/3/22
     */
    public boolean insert(String tableName, String rowKey, String family, String qualifier, String value) {
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @Descripton: 删除
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @Date: 2018/3/22
     */
    public boolean delete(String tableName, String rowKey, String family, String qualifier) {
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            table.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @Descripton: 删除一行
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @Date: 2018/3/22
     */
    public boolean delete(String tableName, String rowKey) {
        return delete(tableName, rowKey, null, null);
    }

    /**
     * @Descripton: 删除一行下的一个列族
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @param family
     * @Date: 2018/3/22
     */
    public boolean delete(String tableName, String rowKey, String family) {
        return delete(tableName, rowKey, family, null);
    }

    /**
     * @Descripton: 数据读取（取到一个值）
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @Date: 2018/3/22
     */
    public String query(String tableName, String rowKey, String family, String qualifier) {
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result r = table.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @Descripton: 取到一个族列的值
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @param family
     * @Date: 2018/3/22
     */
    public Map<String, String> query(String tableName, String rowKey, String family) {
        Map<String, String> result = null ;
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result r = table.get(get);
            List<Cell> cs = r.listCells();
            result = cs.size() > 0 ? new HashMap<String, String>() : result;
            for (Cell cell : cs) {
                result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * @Descripton: 取到多个族列的值
     * @Author: Sorin
     * @param tableName
     * @param rowKey
     * @Date: 2018/3/22
     */
    public Map<String, Map<String, String>> query(String tableName, String rowKey) {
        Map<String, Map<String, String>> results = null ;
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result r = table.get(get);
            List<Cell> cs = r.listCells();
            results = cs.size() > 0 ? new HashMap<String, Map<String, String>> () : results;
            for (Cell cell : cs) {
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                if (results.get(familyName) == null) {
                    results.put(familyName, new HashMap<String,  String> ());
                }
                results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    /**
     * @Descripton: 根据条件过滤查询
     * @Author: Sorin
     * @param obj
     * @param param
     * @Date: 2018/3/26
     */
    public <T> List<T> queryScan(T obj, Map<String, String> param){
        List<T> objs = new ArrayList<T>();
        try {
            String tableName = getORMTable(obj);
            if (StringUtils.isBlank(tableName)) {
                return null;
            }
            Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            for (Map.Entry<String, String> entry : param.entrySet()){
                Filter filter = new SingleColumnValueFilter(Bytes.toBytes(entry.getKey()), null, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(entry.getValue()));
                scan.setFilter(filter);
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T beanClone = (T)BeanUtils.cloneBean(HBaseBeanUtil.resultToBean(result, obj));
                objs.add(beanClone);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objs;
    }

    /**
     * @Descripton: 根据rowkey查询
     * @Author: Sorin
     * @param obj
     * @param rowkeys
     * @Date: 2018/3/22
     */
    public <T> List<T> get(T obj, String... rowkeys) {
        List<T> objs = new ArrayList<T>();
        String tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return objs;
        }
        List<Result> results = getResults(tableName, rowkeys);
        if (results.isEmpty()) {
            return objs;
        }
        for (int i = 0; i < results.size(); i++) {
            T bean = null;
            Result result = results.get(i);
            if (result == null || result.isEmpty()) {
                continue;
            }
            try {
                bean = HBaseBeanUtil.resultToBean(result, obj);
                objs.add(bean);
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
        return objs;
    }


    /**
     * @Descripton: 保存实体对象
     * @Author: Sorin
     * @param objs
     * @Date: 2018/3/22
     */
    public <T> boolean save(T ... objs) {
        List<Put> puts = new ArrayList<Put>();
        String tableName = "";
        for (Object obj : objs) {
            if (obj == null) continue;
            tableName = getORMTable(obj);
            try {
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
        return savePut(puts, tableName);
    }

    /**
     * @Descripton: 根据tableName保存
     * @Author: Sorin
     * @param tableName
     * @param objs
     * @Date: 2018/3/22
     */
    public <T> void save(String tableName, T ... objs){
        List<Put> puts = new ArrayList<Put>();
        for (Object obj : objs) {
            if (obj == null) continue;
            try {
                Put put = HBaseBeanUtil.beanToPut(obj);
                puts.add(put);
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
        savePut(puts, tableName);
    }

    /**
     * @Descripton: 删除
     * @Author: Sorin
     * @param obj
     * @param rowkeys
     * @Date: 2018/3/22
     */
    public <T> void delete(T obj, String... rowkeys) {
        String tableName = "";
        tableName = getORMTable(obj);
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        List<Delete> deletes = new ArrayList<Delete>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            deletes.add(new Delete(Bytes.toBytes(rowkey)));
        }
        delete(deletes, tableName);
    }


    /**
     * @Descripton: 批量删除
     * @Author: Sorin
     * @param deletes
     * @param tableName
     * @Date: 2018/3/22
     */
    private void delete(List<Delete> deletes, String tableName) {
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));) {
            if (StringUtils.isBlank(tableName)) {
                return;
            }
            table.delete(deletes);
        } catch (IOException e) {
            logger.error("删除失败！",e);
        }
    }

    /**
     * @Descripton: 根据tableName获取列簇名称
     * @Author: Sorin
     * @param tableName
     * @Date: 2018/3/22
     */
    public List<String> familys(String tableName) {
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));){
            List<String> columns = new ArrayList<String>();
            if (table==null) {
                return columns;
            }
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor :columnDescriptors) {
                String columnName = columnDescriptor.getNameAsString();
                columns.add(columnName);
            }
            return columns;
        } catch (Exception e) {
            logger.error("查询列簇名称失败！" ,e);
        }
        return new ArrayList<String>();
    }

    // 保存方法
    private boolean savePut(List<Put> puts, String tableName){
        if (StringUtils.isBlank(tableName)) {
            return false;
        }
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));Admin admin = HconnectionFactory.connection.getAdmin();){
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){ // 表不存在
                createTable(tableName, tableName);
            }
            table.put(puts);
            return true;
        }catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 获取tableName
    private String getORMTable(Object obj) {
        HbaseTable table = obj.getClass().getAnnotation(HbaseTable.class);
        return table.tableName();
    }

    // 获取查询结果
    private List<Result> getResults(String tableName, String... rowkeys) {
        List<Result> resultList = new ArrayList<Result>();
        List<Get> gets = new ArrayList<Get>();
        for (String rowkey : rowkeys) {
            if (StringUtils.isBlank(rowkey)) {
                continue;
            }
            Get get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }
        try (Table table = HconnectionFactory.connection.getTable(TableName.valueOf(tableName));) {
            Result[] results = table.get(gets);
            Collections.addAll(resultList, results);
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            return resultList;
        }
    }
}
