package com.muheda.notice.entity;

import com.muheda.notice.hbase.HbaseColumn;
import com.muheda.notice.hbase.HbaseTable;

/**
 * @Author: Sorin
 * @Descriptions:
 * @Date: Created in 2018/3/22
 */
@HbaseTable(tableName="mhd-shop")
public class Demo {

	@HbaseColumn(family="rowkey", qualifier="rowkey")
	private String id;

	@HbaseColumn(family="sorin2", qualifier="conn")
	private String content;

	@HbaseColumn(family="sorin2", qualifier="bytes")
	private String avg;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getAvg() {
		return avg;
	}

	public void setAvg(String avg) {
		this.avg = avg;
	}
}
