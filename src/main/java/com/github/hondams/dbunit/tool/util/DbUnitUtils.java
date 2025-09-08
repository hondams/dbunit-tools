package com.github.hondams.dbunit.tool.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.sql.SQLException;
import lombok.experimental.UtilityClass;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.excel.XlsDataSetWriter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.dbunit.dataset.yaml.YamlDataSet;

@UtilityClass
public class DbUnitUtils {

    //    public IDataSet createEmptyDataSet(Connection connection, String schemaName, String tableName) {
    //        try {
    //            DatabaseNode databaseNode = DatabaseUtils.getDatabaseNode(connection, null, schemaName,
    //                tableName);
    //            if (databaseNode.getCatalogs().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected catalogs size: " + databaseNode.getCatalogs().size());
    //            }
    //            CatalogNode catalogNode = databaseNode.getCatalogs().get(0);
    //            if (catalogNode.getSchemas().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected schemas size: " + catalogNode.getSchemas().size());
    //            }
    //            SchemaNode schemaNode = catalogNode.getSchemas().get(0);
    //            if (schemaNode.getTables().size() != 1) {
    //                throw new IllegalStateException(
    //                    "Unexpected tables size: " + schemaNode.getTables().size());
    //            }
    //            TableNode tableNode = schemaNode.getTables().get(0);
    //
    //            List<String> columnNames = new ArrayList<>();
    //            for (var column : tableNode.getColumns()) {
    //                columnNames.add(column.getColumnName());
    //            }
    //            ITableMetaData
    //            DefaultTable table = new DefaultTable(tableName, columnNames.toArray(new String[0]));
    //            DefaultDataSet dataSet = new DefaultDataSet(table);
    //            return dataSet;
    //        } catch (SQLException e) {
    //            throw new IllegalStateException(e);
    //        }
    //    }
    public IDataSet createDatabaseDataSet(DatabaseConnection databaseConnection) {
        try {
            return databaseConnection.createDataSet();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet createDatabaseDataSet(DatabaseConnection databaseConnection,
        String[] tableNames) {
        try {
            return databaseConnection.createDataSet(tableNames);
        } catch (SQLException | DatabaseUnitException e) {
            throw new IllegalStateException(e);
        }
    }


    public IDataSet load(File file) {
        String extension = getFileExtension(file);
        if ("xml".equalsIgnoreCase(extension) || "flatxml".equalsIgnoreCase(extension)) {
            return loadFlatXml(file);
        } else if ("xls".equalsIgnoreCase(extension) || "xlsx".equalsIgnoreCase(extension)) {
            return loadXls(file);
        } else if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            return loadYaml(file);
        } else {
            throw new IllegalArgumentException("Unsupported file extension: " + extension);
        }
    }

    public IDataSet loadFlatXml(File file) {
        try {
            FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
            builder.setColumnSensing(true);
            return builder.build(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet loadYaml(File file) {
        try {
            return new YamlDataSet(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet loadXls(File file) {
        try {
            return new XlsDataSet(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void save(IDataSet dataSet, File file) {
        String extension = getFileExtension(file);
        if ("xml".equalsIgnoreCase(extension) || "flatxml".equalsIgnoreCase(extension)) {
            saveFlatXml(dataSet, file);
        } else if ("xls".equalsIgnoreCase(extension)) {
            saveXls(dataSet, file);
        } else if ("xlsx".equalsIgnoreCase(extension)) {
            saveXlsx(dataSet, file);
        } else if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            saveYaml(dataSet, file);
        } else {
            throw new IllegalArgumentException("Unsupported file extension: " + extension);
        }
    }

    private String getFileExtension(File file) {
        String fileName = file.getName();
        return fileName.contains(".")//
            ? fileName.substring(fileName.lastIndexOf(".") + 1)//
            : "";
    }

    public void saveFlatXml(IDataSet dataSet, File file) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            FlatXmlWriter writer = new FlatXmlWriter(fos, "UTF-8");
            writer.setIncludeEmptyTable(true);
            writer.setPrettyPrint(true);
            writer.write(dataSet);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveXls(IDataSet dataSet, File file) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            XlsDataSet.write(dataSet, fos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveXlsx(IDataSet dataSet, File file) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            XlsDataSetWriter writer = new XlsDataSetWriter() {
                @Override
                public Workbook createWorkbook() {
                    return new XSSFWorkbook();
                }
            };
            writer.write(dataSet, fos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveYaml(IDataSet dataSet, File file) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            Writer writer = new OutputStreamWriter(fos, java.nio.charset.StandardCharsets.UTF_8);
            YamlDataSet.write(dataSet, writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }
}
