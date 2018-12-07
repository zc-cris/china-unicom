package com.cris.consumer.dao;

import com.cris.api.Column;
import com.cris.api.TableRef;
import com.cris.bean.AbstractHbaseBean;
import com.cris.constant.ConfigConstant;
import com.cris.constant.Names;
import com.cris.consumer.bean.Callee;
import com.cris.consumer.bean.Calllog;
import com.cris.dao.BaseDao;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 专门和 HBase 交互的 dao
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings({"JavaDoc", "SpellCheckingInspection", "unused"})
public class HbaseDao extends BaseDao {

    /**
     * 协助当前线程存储数据到一个该线程的map中，key 就是当前 ThreadLocal 对象，值就是要存储的数据
     **/
    private static final ThreadLocal<Connection> CONNECTION_THREAD_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<Admin> ADMIN_THREAD_LOCAL = new ThreadLocal<>();
    /**
     * 默认的列族版本最大信息为1
     **/
    private static final int DEFAULT_MAX_VERSION = 1;

    /**
     * 获取和 HBase 的连接
     *
     * @return 当前线程绑定的 Connection 对象
     */
    private Connection getConnection() {
        Optional<Connection> optionalConnection = Optional.ofNullable(CONNECTION_THREAD_LOCAL.get());

        /*如果 optionalConnection 有值，那么返回容器中的值，否则为当前线程的 ThreadLocalMap 存储一个 connection*/
        return optionalConnection.orElseGet(() -> {
            try {
                /*创建 Connection 过程中报错就直接抛出空指针异常，确保每个当前线程都应该有 connection
                 * 并且需要保证从当前线程得到的 onnection 一定是非空值*/
                Connection conn = ConnectionFactory.createConnection();
                CONNECTION_THREAD_LOCAL.set(conn);
                return conn;
            } catch (IOException e) {
                e.printStackTrace();
                throw new NullPointerException();
            }
        });
    }

    /**
     * 获取 Admin，具体的实现逻辑同 getConnection 方法
     *
     * @return 当前线程存储的 Admin 对象
     */
    private Admin getAdmin() {
        Optional<Admin> optionalAdmin = Optional.ofNullable(ADMIN_THREAD_LOCAL.get());
        return optionalAdmin.orElseGet(() -> {
            try {
                Admin admin = getConnection().getAdmin();
                ADMIN_THREAD_LOCAL.set(admin);
                return admin;
            } catch (IOException e) {
                e.printStackTrace();
                throw new NullPointerException();
            }
        });
    }

    /**
     * 初始化 HBase 的命名空间和表(每次测试生成新的表)
     */
    public void init() throws IOException {
        start();

        /*初始化命名空间，存在就什么都不做，否则创建命名空间*/
        initNameSpaceNX(Names.NAMESPACE.getValue());

        /*初始化和删除表，测试用*/
        initTableXX(Names.TABLE_NAME.getValue(), Names.CF_CALLER.getValue(), Names.CF_CALLEE.getValue());

//        end();
    }

    /**
     * 初始化 HBase 表，存在就删除，不存在就创建（仅供测试）
     */
    private void initTableXX(String tableName, String... colFamilyNames) throws IOException {
        removeTable(tableName);
        /*这里通过读取配置文件的方式读取分区数目*/
        initTable(tableName, ConfigConstant.CONFIG_MAP.get("coprocessor.className"), Integer.parseInt(ConfigConstant.CONFIG_MAP.get("calllog.regionCount")), colFamilyNames);
    }

    /**
     * 初始化命名空间
     *
     * @param nameSpaceName 命名空间名字
     * @throws IOException
     */
    private void initNameSpaceNX(String nameSpaceName) throws IOException {
        Admin admin = getAdmin();
        // 先要判断 nameSpace 是否存在，如果不存在那么创建
        if (!existNameSpace(nameSpaceName)) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpaceName).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 判断命名空间是否存在
     *
     * @param nameSpace 命名空间名字
     * @return 命名空间存在返回 true
     * @throws IOException
     */
    @SuppressWarnings("unused")
    private boolean existNameSpace(String nameSpace) {
        Admin admin = getAdmin();
        try {
            /*如果命名空间不存在将会抛出 NamespaceNotFoundException */
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(nameSpace);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 移除命名空间
     *
     * @param nameSpaceName 命名空间名字
     * @throws IOException
     */
    @SuppressWarnings("unused")
    protected void removeNameSpace(String nameSpaceName) throws IOException {
        Admin admin = getAdmin();
        // 先要判断 nameSpace 是否存在，如果存在那么删除
        boolean existNameSpace = existNameSpace(nameSpaceName);
        if (existNameSpace) {
            admin.deleteNamespace(nameSpaceName);
        }
    }

    /**
     * 移除 HBase 表
     *
     * @param tableName 表名
     * @throws IOException
     */
    private void removeTable(String tableName) throws IOException {
        Admin admin = getAdmin();
        TableName name = TableName.valueOf(tableName);
        boolean tableExists = admin.tableExists(name);
        if (tableExists) {
            /*先要更新表状态为不可用才能删除 HBase 的表*/
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }

    /**
     * 初始化项目所需要的 HBase 表
     *
     * @param tableName
     * @param colFamilyNames
     * @param regionCount
     * @param coprocessorClassName
     * @throws IOException
     */
    private void initTable(String tableName, String coprocessorClassName, int regionCount, String... colFamilyNames) throws IOException {
        Admin admin = getAdmin();
        createTable(admin, tableName, coprocessorClassName, regionCount, colFamilyNames);
    }

    /**
     * 实际的创建 HBase 表的方法
     *
     * @param admin                HBase 集群的主机（HMaster 节点），负责协调 HBase 集群
     * @param tableName            表名
     * @param colFamilyNames       列族（可能多个）
     * @param regionCount          分区数（region 的个数）
     * @param coprocessorClassName 协处理器
     * @throws IOException
     */
    private void createTable(Admin admin, String tableName, String coprocessorClassName, int regionCount, String... colFamilyNames) throws IOException {
        TableName name = TableName.valueOf(tableName);
        boolean tableExists = admin.tableExists(name);

        // 如果不传入列族名字，使用默认的即可
        if (colFamilyNames == null || colFamilyNames.length == 0) {
            colFamilyNames = new String[1];
            colFamilyNames[0] = Names.DEFAULT_CF.getValue();
        }

        if (!tableExists) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            for (String cfName : colFamilyNames) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfName);
                hColumnDescriptor.setMaxVersions(DEFAULT_MAX_VERSION);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            /*添加协处理器*/
            if (StringUtils.isNotEmpty(coprocessorClassName.trim())) {
                hTableDescriptor.addCoprocessor(coprocessorClassName);
            }

            /*增加预分区*/
            if (regionCount <= 0) {
                /*HBase 默认分区数为 1 */
                admin.createTable(hTableDescriptor);
            } else {
                /*分区键处理*/
                byte[][] splitKeys = genSplitKeys(regionCount);
                admin.createTable(hTableDescriptor, splitKeys);
            }
        }
    }

    /**
     * 生产分区键（重要～～～）
     *
     * @param regionCount 分区数
     * @return
     */
    private byte[][] genSplitKeys(int regionCount) {
        // 这里的分区键的数量计算由分区数得到：例如我们想要 6 个分区，实际上只需要 5 个分区键即可
        int splitKeysCount = regionCount - 1;
        byte[][] splitKeys = new byte[splitKeysCount][];

        /*分区键的比较根据字符串的编码比较（rowkey 都是字符串类型）
         * 不同的 rowkey 和分区键比较以后进入不同的分区*/
        // 0,1,2 三个分区键，四个分区
        // 000012/132323/20000/34566,6666
        // (-∞，0|),[1,1|),[2,2|),[3,+∞)
        List<byte[]> list = new ArrayList<>();
        String key;
        for (int i = 0; i < splitKeysCount; i++) {
            key = i + "|";
            list.add(Bytes.toBytes(key));
        }

        /// 需要对生成的分区键进行排序，利用 HBase 写好的工具类排序即可，我们这里得到的分区键已经是排好序的了
//        list.sort(new Bytes.ByteArrayComparator());

        return list.toArray(splitKeys);
    }


    /**
     * 执行流程的开始,获取 Connection 和 Admin 对象
     */
    @Override
    protected void start() {
        getConnection();
        getAdmin();
    }

    /**
     * 执行流程的结束，关闭 Connection 和 Admin 对象
     */
    @Override
    public void end() throws IOException {
        Optional<Admin> optionalAdmin = Optional.of(getAdmin());
        optionalAdmin.get().close();
        CONNECTION_THREAD_LOCAL.remove();

        Optional<Connection> optionalConnection = Optional.of(getConnection());
        optionalConnection.get().close();
        ADMIN_THREAD_LOCAL.remove();
    }

    /**
     * 根据指定的时间范围从HBase 表查询数据（这里我们以月份作为范围）
     * 例如：想要查询 201808 ～ 201902 的数据
     *
     * @param tell1         主叫用户
     * @param startDateTime 查询的开始日期，格式：201909XXXXXXXX,至少 6 位包含年月即可
     * @param endDateTime   查询的结束日期，格式：201909XXXXXXXX,至少 6 位包含年月即可
     */
    List<byte[][]> getStartRowKeyAndStopRowKey(String tell1, String startDateTime, String endDateTime) throws ParseException {

        /*从 HBase 表查询数据，scan 是关键，根据 rowkey 来查询数据更加快，如果使用 filter 来过滤列，就太慢了！*/

        // 得到 201908 这样格式的字符串
        String startDate = startDateTime.substring(0, 6);
        String endDate = endDateTime.substring(0, 6);

        Calendar startCalendar = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        startCalendar.setTime(simpleDateFormat.parse(startDate));

        Calendar stopCalendar = Calendar.getInstance();
        stopCalendar.setTime(simpleDateFormat.parse(endDate));

        if (stopCalendar.compareTo(startCalendar) < 0) {
            throw new IllegalArgumentException("查询结束时间不能早于开始时间！！！");
        }
        List<byte[][]> list = new ArrayList<>();
        while (stopCalendar.compareTo(startCalendar) >= 0) {
            // 1. 先根据主叫号码的后四位和时间的年月得到分区号
            String date = simpleDateFormat.format(startCalendar.getTime());
            int regionNum = genRegionNum(tell1, date);
            String startRowKey = regionNum + "_" + tell1 + "_" + date + "_";
            String stopRowKey = startRowKey + "|";

            byte[][] bytes = new byte[][]{Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey)};
            list.add(bytes);
            startCalendar.add(Calendar.MONTH, 1);
        }

        return list;
    }

    /**
     * 通过注解更加方便的插入数据到 HBase 表的方法，扩展性更强（重点！！！）
     *
     * @param calllog 主叫记录对象
     * @param callee  被叫记录对象
     */
    public void insertValue(AbstractHbaseBean calllog, AbstractHbaseBean callee) throws IOException {
        Calllog caller = (Calllog) calllog;
        Callee callee1 = (Callee) callee;

        /*计算每个记录对象的 regionNum */
        caller.setRegionNum(genRegionNum(caller.getCall1(), caller.getCalltime()));
        callee1.setRegionNum(genRegionNum(callee1.getCall1(), callee1.getCalltime()));
        putData(calllog, callee1);
    }

    /**
     * 实际将对象数据新增到 HBase 表中的方法,使用反射和注解提高开发效率（重难点！！！）
     *
     * @param caller 主叫用户的记录对象
     * @param callee 被叫用户的记录对象
     */
    private void putData(AbstractHbaseBean caller, AbstractHbaseBean callee) throws IOException {

        /*通过注解获取表名*/
        Class<? extends AbstractHbaseBean> callerClass = caller.getClass();
        Optional<TableRef> annotation = Optional.of(callerClass.getAnnotation(TableRef.class));
        String tableName = annotation.get().value();

        // 得到 HBase 表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));

        /*得到主叫记录的 rowkey */
        String callerRowKey = caller.getRowKey();
        Put putCaller = new Put(Bytes.toBytes(callerRowKey));

        /*得到被叫记录的 rowkey*/
        Class<? extends AbstractHbaseBean> calleeClass = callee.getClass();
        String calleeRowKey = callee.getRowKey();
        Put putCallee = new Put(Bytes.toBytes(calleeRowKey));
        initColumns(callee, calleeClass, putCallee);

        initColumns(caller, callerClass, putCaller);

        // put
        ArrayList<Put> calllogs = new ArrayList<>();
        calllogs.add(putCaller);
        calllogs.add(putCallee);

        table.put(calllogs);
        table.close();
    }

    public void initColumns(AbstractHbaseBean bean, Class<? extends AbstractHbaseBean> clazz, Put put) {
        /*得到列族，列名，列值*/
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            Optional<Column> optionalColumn = Optional.ofNullable(field.getAnnotation(Column.class));
            optionalColumn.ifPresent(column -> {
                String columnFamily = column.columnFamily();
                String colName = column.column();
                if (StringUtils.isEmpty(colName.trim())) {
                    colName = field.getName();
                }
                field.setAccessible(true);
                try {
                    Optional<String> optional = Optional.of((String) field.get(bean));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), Bytes.toBytes(optional.get()));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * 定义插入数据的方法(重要！！！)
     *
     * @param data 需要插入的数据
     */
    public void insertValue(String data) throws IOException {
        // 将采集到的通话记录放入到 HBase 表中

        //1. 数据分解
        String[] strings = data.split("\t");

        //2. 组装 put 对象
        /*rowkey 设计：
         * 1. 长度原则：最大值 64 kb，推荐长度为 10 ～ 100 byte，最好是 8 的倍数（64 位系统好处理），能短则短
         * 2. 唯一原则：rowkey 唯一
         * 3. 散列原则：
         *   - 盐值加密：不能使用时间戳作为 rowkey（导致热点数据-》数据倾斜），需要在 rowkey 前增加随机数
         *   - 字符串反转：时间戳和电话号码；电话号码最后四位才是用户的编码（没有规律）
         *   - 计算分区号
         * */
        // 我们这里的 rowkey 格式应该为：regionNum（分区号） + call1+call2+calltime+duration（实际开发中以实际业务需求为准设计 rowkey）
        /*分区号的设计是让有共同规律的数据进入同一个分区，实现有规律的存储和提高查询效率*/
        int rowkey = genRegionNum(strings[0], strings[2]);

        String call1 = strings[0];
        String call2 = strings[1];
        String calltime = strings[2];
        String duration = strings[3];

        // rowkey
        Put put = new Put(Bytes.toBytes(rowkey + "_" + call1 + "_" + call2 + "_" + calltime + "_" + duration+"_1"));
        // 列族
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
        // 列（名字和值）
        put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //3. 保存数据到 HBase 表
        putData(Names.TABLE_NAME.getValue(), put);
    }

    /**
     * 计算分区号（这里通过主叫号码和通话的年月来将相似的数据放入到同一个分区）
     *
     * @param tell1 主叫电话，字符串类型
     * @param date  时间字符串，20180808080808 这种类型
     * @return 分区号
     */
    public int genRegionNum(String tell1, String date) {
        // 用户电话号码的后四位没有规律 13875644321
        String userCode = tell1.substring(tell1.length() - 4);
        // 201807
        String yearMonth = date.substring(0, 6);

        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc 校验（异或算法）
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模得到分区号
        return crc % Integer.parseInt(ConfigConstant.CONFIG_MAP.get("calllog.regionCount"));
    }


    /**
     * 向 HBase 表增加数据
     *
     * @param tableName
     * @param put
     * @throws IOException
     */
    private void putData(String tableName, Put put) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
    }

}
