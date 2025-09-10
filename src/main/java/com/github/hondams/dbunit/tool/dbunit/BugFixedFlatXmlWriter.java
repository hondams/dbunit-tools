package com.github.hondams.dbunit.tool.dbunit;

/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.util.xml.XmlWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.5.5 (Apr 19, 2003)
 */
public class BugFixedFlatXmlWriter implements IDataSetConsumer {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(
        BugFixedFlatXmlWriter.class);

    private static final String DATASET = "dataset";

    private XmlWriter _xmlWriter;
    private ITableMetaData _activeMetaData;
    private int _activeRowCount;
    private boolean _includeEmptyTable = false;
    private String _systemId = null;

    public BugFixedFlatXmlWriter(OutputStream out) throws IOException {
        this(out, null);
    }

    /**
     * @param outputStream
     *     The stream to which the XML will be written.
     * @param encoding
     *     The encoding to be used for the {@link XmlWriter}. Can be null. See
     *     {@link XmlWriter#XmlWriter(OutputStream, String)}.
     * @throws java.io.UnsupportedEncodingException
     */
    public BugFixedFlatXmlWriter(OutputStream outputStream, String encoding)
        throws UnsupportedEncodingException {
        this._xmlWriter = new BugFixedXmlWriter(outputStream, encoding);
        this._xmlWriter.enablePrettyPrint(true);
    }

    public BugFixedFlatXmlWriter(Writer writer) {
        this._xmlWriter = new BugFixedXmlWriter(writer);
        this._xmlWriter.enablePrettyPrint(true);
    }

    public BugFixedFlatXmlWriter(Writer writer, String encoding) {
        this._xmlWriter = new BugFixedXmlWriter(writer, encoding);
        this._xmlWriter.enablePrettyPrint(true);
    }

    public void setIncludeEmptyTable(boolean includeEmptyTable) {
        this._includeEmptyTable = includeEmptyTable;
    }

    public void setDocType(String systemId) {
        this._systemId = systemId;
    }

    /**
     * Enable or disable pretty print of the XML.
     *
     * @param enabled
     *     <code>true</code> to enable pretty print (which is the default).
     *     <code>false</code> otherwise.
     * @since 2.4
     */
    public void setPrettyPrint(boolean enabled) {
        this._xmlWriter.enablePrettyPrint(enabled);
    }

    /**
     * Writes the given {@link org.dbunit.dataset.IDataSet} using this writer.
     *
     * @param dataSet
     *     The {@link org.dbunit.dataset.IDataSet} to be written
     * @throws org.dbunit.dataset.DataSetException
     */
    public void write(IDataSet dataSet) throws DataSetException {
        logger.debug("write(dataSet={}) - start", dataSet);

        DataSetProducerAdapter provider = new DataSetProducerAdapter(dataSet);
        provider.setConsumer(this);
        provider.produce();
    }

    /// /////////////////////////////////////////////////////////////////////////
    // IDataSetConsumer interface
    @Override
    public void startDataSet() throws DataSetException {
        logger.debug("startDataSet() - start");

        try {
            this._xmlWriter.writeDeclaration();
            this._xmlWriter.writeDoctype(this._systemId, null);
            this._xmlWriter.writeElement(DATASET);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        logger.debug("endDataSet() - start");

        try {
            this._xmlWriter.endElement();
            this._xmlWriter.close();
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        logger.debug("startTable(metaData={}) - start", metaData);

        this._activeMetaData = metaData;
        this._activeRowCount = 0;
    }

    @Override
    public void endTable() throws DataSetException {
        logger.debug("endTable() - start");

        if (this._includeEmptyTable && this._activeRowCount == 0) {
            try {
                String tableName = this._activeMetaData.getTableName();
                this._xmlWriter.writeEmptyElement(tableName);
            } catch (IOException e) {
                throw new DataSetException(e);
            }
        }

        this._activeMetaData = null;
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        logger.debug("row(values={}) - start", values);

        try {
            String tableName = this._activeMetaData.getTableName();
            this._xmlWriter.writeElement(tableName);

            Column[] columns = this._activeMetaData.getColumns();
            for (int i = 0; i < columns.length; i++) {
                String columnName = columns[i].getColumnName();
                Object value = values[i];

                // Skip null value
                if (value == null) {
                    continue;
                }

                try {
                    String stringValue = DataType.asString(value);
                    this._xmlWriter.writeAttribute(columnName, stringValue, true);
                } catch (TypeCastException e) {
                    throw new DataSetException(
                        "table=" + this._activeMetaData.getTableName() + ", row=" + i + ", column="
                            + columnName + ", value=" + value, e);
                }
            }

            this._activeRowCount++;
            this._xmlWriter.endElement();
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }
}
