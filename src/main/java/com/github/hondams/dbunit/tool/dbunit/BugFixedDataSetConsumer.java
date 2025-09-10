package com.github.hondams.dbunit.tool.dbunit;

import lombok.RequiredArgsConstructor;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

@RequiredArgsConstructor
public class BugFixedDataSetConsumer implements IDataSetConsumer {

    private final IDataSetConsumer delegate;

    @Override
    public void startDataSet() throws DataSetException {
        this.delegate.startDataSet();
    }

    @Override
    public void endDataSet() throws DataSetException {
        this.delegate.endDataSet();
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        this.delegate.startTable(metaData);
    }

    @Override
    public void endTable() throws DataSetException {
        this.delegate.endTable();
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] instanceof String) {
                    String v = (String) values[i];
                    // BugFixedXmlWriterと同様の処理を実施
                    v = v.replace("&#x0;", "\u0000"); // エスケープしたnull文字を復元する。
                    values[i] = v;
                }
            }
        }
        this.delegate.row(values);
    }
}
