package com.github.hondams.dbunit.tool.dbunit;

import com.github.hondams.dbunit.tool.util.FileUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvDataSet;
import org.dbunit.dataset.csv.CsvDataSetWriter;
import org.dbunit.dataset.csv.CsvProducer;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.excel.XlsDataSetWriter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.dataset.xml.XmlProducer;
import org.dbunit.dataset.yaml.YamlDataSet;
import org.dbunit.dataset.yaml.YamlProducer;
import org.xml.sax.InputSource;

@UtilityClass
@Slf4j
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
        String extension = FileUtils.getFileExtension(file);
        if ("xml".equalsIgnoreCase(extension)) {
            try {
                return loadFlatXml(file);
            } catch (Exception e) {
                return loadXml(file);
            }
        } else if ("flatxml".equalsIgnoreCase(extension)) {
            return loadFlatXml(file);
        } else if ("xls".equalsIgnoreCase(extension) || "xlsx".equalsIgnoreCase(extension)) {
            return loadXls(file);
        } else if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            return loadYaml(file);
        } else if (file.isDirectory()) {
            File tableOrderingFile = new File(file, CsvDataSet.TABLE_ORDERING_FILE);
            if (!tableOrderingFile.exists()) {
                throw new IllegalArgumentException(
                    "When input is directory, it must contain " + CsvDataSet.TABLE_ORDERING_FILE
                        + ": " + file.getAbsolutePath());
            }
            return loadCsv(file);
        } else {
            throw new IllegalArgumentException("Unsupported file extension: " + extension);
        }
    }


    public IDataSet loadStreaming(File file) {
        String extension = FileUtils.getFileExtension(file);
        if ("xml".equalsIgnoreCase(extension)) {
            try {
                return loadStreamingFlatXml(file);
            } catch (Exception e) {
                return loadStreamingXml(file);
            }
        } else if ("flatxml".equalsIgnoreCase(extension)) {
            return loadStreamingFlatXml(file);
        } else if ("xls".equalsIgnoreCase(extension) || "xlsx".equalsIgnoreCase(extension)) {
            return loadXls(file);
        } else if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            return loadStreamingYaml(file);
        } else if (file.isDirectory()) {
            File tableOrderingFile = new File(file, CsvDataSet.TABLE_ORDERING_FILE);
            if (!tableOrderingFile.exists()) {
                throw new IllegalArgumentException(
                    "When input is directory, it must contain " + CsvDataSet.TABLE_ORDERING_FILE
                        + ": " + file.getAbsolutePath());
            }
            return loadStreamingCsv(file);
        } else {
            throw new IllegalArgumentException("Unsupported file extension: " + extension);
        }
    }

    public IDataSet loadXml(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            return new XmlDataSet(fis);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet loadStreamingFlatXml(File file) {
        return new ReCallableStreamingDataSet(() -> {
            try {
                Reader reeder = new InputStreamReader(new FileInputStream(file),
                    StandardCharsets.UTF_8);
                InputSource inputSource = new InputSource(reeder);
                FlatXmlProducer producer = new FlatXmlProducer(inputSource);
                producer.setColumnSensing(true);
                return new ClosableDataSetProducer(producer, () -> {
                    try {
                        reeder.close();
                    } catch (IOException e) {
                        log.debug("Failed to close reader", e);
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        });
    }

    public IDataSet loadStreamingYaml(File file) {
        return new ReCallableStreamingDataSet(() -> {
            try {
                return new YamlProducer(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public IDataSet loadStreamingXml(File file) {
        return new ReCallableStreamingDataSet(() -> {
            try {
                Reader reeder = new InputStreamReader(new FileInputStream(file),
                    StandardCharsets.UTF_8);
                InputSource inputSource = new InputSource(reeder);
                XmlProducer producer = new XmlProducer(inputSource);
                return new ClosableDataSetProducer(producer, () -> {
                    try {
                        reeder.close();
                    } catch (IOException e) {
                        log.debug("Failed to close reader", e);
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        });
    }

    public IDataSet loadStreamingCsv(File file) {
        return new ReCallableStreamingDataSet(() -> new CsvProducer(file));
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

    public IDataSet loadCsv(File dir) {
        try {
            return new CsvDataSet(dir);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void save(IDataSet dataSet, File file, String format) {
        String extension = FileUtils.getFileExtension(file);
        if ("xml".equalsIgnoreCase(extension)) {
            if (format == null || "flatxml".equalsIgnoreCase(format)) {
                saveFlatXml(dataSet, file);
            } else if ("xml".equalsIgnoreCase(format)) {
                saveXml(dataSet, file);
            } else {
                throw new IllegalArgumentException(
                    "When file extension is xml, format must be xml or flatxml: " + format);
            }
        } else if ("flatxml".equalsIgnoreCase(extension)) {
            if (format != null && !"flatxml".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException(
                    "When file extension is flatxml, format must be flatxml: " + format);
            }
            saveFlatXml(dataSet, file);
        } else if ("xls".equalsIgnoreCase(extension)) {
            if (format != null && !"excel".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException(
                    "When file extension is xls, format must be excel: " + format);
            }
            saveXls(dataSet, file);
        } else if ("xlsx".equalsIgnoreCase(extension)) {
            if (format != null && !"excel".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException(
                    "When file extension is xlsx, format must be excel: " + format);
            }
            saveXlsx(dataSet, file);
        } else if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            if (format != null && !"yaml".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException(
                    "When file extension is yaml or yml, format must be yaml: " + format);
            }
            saveYaml(dataSet, file);
        } else if ("csv".equalsIgnoreCase(format)) {
            checkAndResetCsvDirectory(file);
            saveCsv(dataSet, file);
        } else {
            throw new IllegalArgumentException("Unsupported file extension: " + extension);
        }
    }

    private void checkAndResetCsvDirectory(File file) {
        if (file.exists()) {
            if (!file.isDirectory()) {
                throw new IllegalArgumentException(
                    "When format is csv, output must be a directory: " + file.getAbsolutePath());
            }

            File tableOrderingFile = new File(file, CsvDataSet.TABLE_ORDERING_FILE);
            File[] files = file.listFiles();
            if (tableOrderingFile.exists()) {
                if (files != null) {
                    for (File f : files) {
                        if (f.isDirectory()) {
                            throw new IllegalArgumentException(
                                "When format is csv and exists " + CsvDataSet.TABLE_ORDERING_FILE
                                    + ", output directory must not contain sub directory: "
                                    + file.getAbsolutePath());
                        } else {
                            if (!f.getName().equals(CsvDataSet.TABLE_ORDERING_FILE)) {
                                String fileExtension = FileUtils.getFileExtension(f);
                                if (!"csv".equalsIgnoreCase(fileExtension)) {
                                    throw new IllegalArgumentException(
                                        "When format is csv and exists "
                                            + CsvDataSet.TABLE_ORDERING_FILE
                                            + ", output directory must not contain file other than csv: "
                                            + file.getAbsolutePath());
                                }
                            }
                        }
                    }
                    for (File f : files) {
                        boolean deleted = f.delete();
                        if (!deleted) {
                            throw new IllegalArgumentException(
                                "Failed to delete file: " + f.getAbsolutePath());
                        }
                    }
                }
            } else {
                if (files != null && files.length > 0) {
                    throw new IllegalArgumentException(
                        "When format is csv and not exists " + CsvDataSet.TABLE_ORDERING_FILE
                            + ", output directory must be empty: " + file.getAbsolutePath());
                }
            }
        } else {
            boolean created = file.mkdirs();
            if (!created) {
                throw new IllegalArgumentException(
                    "Failed to create directory: " + file.getAbsolutePath());
            }
        }
    }

    public void saveXml(IDataSet dataSet, File file) {
        try (Writer writer = new OutputStreamWriter(//
            new FileOutputStream(file), StandardCharsets.UTF_8)) {
            XmlDataSet.write(dataSet, writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveFlatXml(IDataSet dataSet, File file) {
        try (Writer w = new OutputStreamWriter(//
            new FileOutputStream(file), StandardCharsets.UTF_8)) {
            BugFixedFlatXmlWriter writer = new BugFixedFlatXmlWriter(w);
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
        try (Writer writer = new OutputStreamWriter(//
            new FileOutputStream(file), StandardCharsets.UTF_8)) {
            YamlDataSet.write(dataSet, writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveCsv(IDataSet dataSet, File dir) {
        try {
            CsvDataSetWriter writer = new CsvDataSetWriter(dir);
            writer.write(dataSet);
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }

    public IDataSet merge(List<IDataSet> dataSets) {
        try {
            return new CompositeDataSet(dataSets.toArray(new IDataSet[0]));
        } catch (DataSetException e) {
            throw new IllegalStateException(e);
        }
    }
}
