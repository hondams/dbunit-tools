package com.github.hondams.dbunit.tool.dbunit;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.stream.StreamingIterator;

@Slf4j
@RequiredArgsConstructor
public class ReCallableStreamingDataSet extends AbstractDataSet {

    private final Supplier<IDataSetProducer> producerFactory;

    @Override
    protected ITableIterator createIterator(boolean reversed) throws DataSetException {
        log.debug("createIterator(reversed={}) - start", reversed);

        return new StreamingIterator(this.producerFactory.get());
    }

    @Override
    public String[] getTableNames() throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        log.debug("getTableMetaData(tableName={}) - start", tableName);

        throw new UnsupportedOperationException();
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        log.debug("getTable(tableName={}) - start", tableName);

        throw new UnsupportedOperationException();
    }
}
