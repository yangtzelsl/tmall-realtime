package com.yangtzelsl.tmall.realtime.utils;

import com.yangtzelsl.tmall.realtime.common.TmallConfig;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PhoenixUtil {

    private static Connection connection;

    /**
     * 查询 一个集合数据
     * @param sql sql
     * @param clazz 返回集合的类型
     * @param <T> 返回集合的类型
     * @return 结果集合
     */
    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (connection == null) {
            initConnection();
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            ArrayList<T> resList = new ArrayList<>();
            while (resultSet.next()) {
                T t = clazz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i), resultSet.getObject(i));
                }
                resList.add(t);
            }
            resultSet.close();
            return resList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * 初始化 Phoenix 连接
     */
    @SneakyThrows
    private static void initConnection() {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(TmallConfig.PHOENIX_SERVER);
    }

}

