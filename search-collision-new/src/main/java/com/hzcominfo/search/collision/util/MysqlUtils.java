package com.hzcominfo.search.collision.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.logger.Loggable;

public class MysqlUtils implements Loggable {

	public static boolean insert(Connection conn, String sql) {
		try (PreparedStatement pst = conn.prepareStatement(sql);) {
			return pst.execute();
		} catch (SQLException e) {
//			logger().error("mysql insert failed", e);
			throw new RuntimeException("mysql insert error: " + e);
		} 
	}
	
	public static int update(Connection conn, String sql) {
		try (PreparedStatement pst = conn.prepareStatement(sql);) {
			return pst.executeUpdate();
		} catch (SQLException e) {
//			logger().error("mysql update failed", e);
			throw new RuntimeException("mysql update error: " + e);
		} 
	}
	
	public static Map<String, Object> selectOne(Connection conn, String sql) {
		Map<String, Object> map = new HashMap<String, Object>();
		try (PreparedStatement pst = conn.prepareStatement(sql);) {
			ResultSet rs = pst.executeQuery();
			ResultSetMetaData rdata = rs.getMetaData();
			int col = rdata.getColumnCount();
			while (rs.next()) {
				for (int i = 1; i <= col; i++) {
					map.put(rdata.getColumnName(i), rs.getObject(rdata.getColumnName(i)));
				}
			}
		} catch (SQLException e) {
//			logger().error("mysql select failed", e);
			throw new RuntimeException("mysql select one error: " + e);
		} 
		return map;
	}
	
	public static List<Map<String, Object>> select(Connection conn, String sql) {
		List<Map<String, Object>> mapList = new ArrayList<>();
		try (PreparedStatement pst = conn.prepareStatement(sql);) {
			ResultSet rs = pst.executeQuery();
			ResultSetMetaData rdata = rs.getMetaData();
			int col = rdata.getColumnCount();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= col; i++) {
					map.put(rdata.getColumnName(i), rs.getObject(rdata.getColumnName(i)));
				}
				mapList.add(map);
			}
		} catch (SQLException e) {
//			logger().error("mysql select failed", e);
			throw new RuntimeException("mysql select error: " + e);
		}
		return mapList;
	}
}
