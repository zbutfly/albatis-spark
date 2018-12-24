package com.hzcominfo.search.collision.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

/**
 * @author cominfo3
 *
 */
public class ExportExcelFile {
	
	private SXSSFWorkbook wb;
	private Sheet sheet;
	private Map<String, CellStyle> styles;
	private int rownum;
	
	
	public void exportExcelFile(String[] title, String sheetName, List<Map<String, Object>> mapList) {
		if(title != null && title.length > 0) {
			String[][] head = new String[mapList.size() + 1][title.length];
			for(int i = 0; i < head[0].length; i++) {
				head[0][i] = title[i];
			}
			List<String> titleStr = Arrays.asList(title);
			int i = 1;
			for(Map<String, Object> map : mapList) {
				if(map != null && map.size() > 0) {
					for(String key : map.keySet()) {
						if(map.get(key) != null)
							head[i][titleStr.indexOf(key)] = map.get(key).toString();
					}
				}
				i++;
			}
			initialize("", head, sheetName, mapList.size() + 1);
		}
	}
	
	private void initialize(String title, String[][] headers, String sheetName, int size) {
		this.wb = new SXSSFWorkbook(size);
		this.sheet = wb.createSheet(sheetName);
		sheet.setDefaultColumnWidth(22);
		this.styles = createStyles(wb);
		if(StringUtils.isNotBlank(title)) {
			Row titleRow = sheet.createRow(rownum++);
			Cell titleCell = titleRow.createCell(0);
			titleCell.setCellStyle(styles.get("title"));
			titleCell.setCellValue(title);
			sheet.addMergedRegion(new CellRangeAddress(titleRow.getRowNum(),
					titleRow.getRowNum(), titleRow.getRowNum(), headers[0].length - 1));
		}
		for(int i = 0; i < headers.length; i++) {
			Row row = sheet.createRow(rownum++);
			for(int j = 0; j < headers[i].length; j++) {
				Cell cell = row.createCell(j);
				if(i == 0) cell.setCellStyle(styles.get("title"));
				else cell.setCellStyle(styles.get("data"));
				if(headers[i][j] != null) {
					cell.setCellValue(headers[i][j]);
				}
				//sheet.autoSizeColumn(j, true);
			}
		}
	}
	
	private Map<String, CellStyle> createStyles(SXSSFWorkbook wb) {
		Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		//title
		CellStyle style = wb.createCellStyle();
		Font titleFont = wb.createFont();
		titleFont.setFontName("宋体");
		titleFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
		style.setFont(titleFont);
		style.setAlignment(CellStyle.ALIGN_CENTER);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		styles.put("title", style);
		//冻结首行
		sheet.createFreezePane(0, 1);
		//data
		style = wb.createCellStyle();
		Font dataFont = wb.createFont();
		dataFont.setFontName("宋体");
		style.setFont(dataFont);
		style.setAlignment(CellStyle.ALIGN_LEFT);
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		//style.setWrapText(true);
		styles.put("data", style);
		return styles;
	}
	
	public ExportExcelFile write(OutputStream os) throws IOException {
		wb.write(os);
		return this;
	}
	
	public ExportExcelFile write(HttpServletResponse response, String fileName) throws IOException {
		response.reset();
		response.setContentType("application/octet-stream; charset=utf-8");
		response.setHeader("Content-Disposition", "attachment; filename=" + urlEncode(fileName));
		write(response.getOutputStream());
		return this;
	}
	
	public ExportExcelFile writeFile(String name) {
		FileOutputStream os = null;
		try {
			os = new FileOutputStream(name);
			this.write(os);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(os != null) os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return this;
	}
	
	public ExportExcelFile dispose() {
		wb.dispose();
		return this;
	}
	
	public String urlEncode(String part) throws UnsupportedEncodingException {
		return URLEncoder.encode(part, "UTF-8");
	}
	
	public static void main(String[] args) {
		/*String[] title = {"name", "age"};
		String sheetName = "人员信息";
		List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
		Map<String, Object> map1 = new HashMap<String, Object>();
		map1.put("name", "chenw1");
		map1.put("age", 1);
		Map<String, Object> map2 = new HashMap<String, Object>();
		map2.put("name", "chenw2");
		map2.put("age", 2);
		mapList.add(map1);
		mapList.add(map2);
		Long ll = new Date().getTime();
		String file = "D:\\EXPORT_" + ll + ".xls";
		String fileStr ="EXPORT_" + 11;
		exportExcelFile(title, sheetName, mapList, file, fileStr);*/
		String[][] heads = new String[2][2];
		heads[0][0] = "name";
		heads[0][1] = "age";
		heads[1][0] = "chenw";
		heads[1][1] = "23";
		//ExportExcelFile ee = new ExportExcelFile("", heads, "xx", heads.length);
		//ee.writeFile("D:EXPORT_" + new Date().getTime() + ".xlsx");
		//ee.dispose();
	}
}
