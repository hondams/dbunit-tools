package com.github.hondams.dbunit.tool.dbunit;

import lombok.RequiredArgsConstructor;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

@RequiredArgsConstructor
public class ClosableDataSetProducer implements IDataSetProducer {

    private final IDataSetProducer delegate;
    private final Runnable onClose;

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        this.delegate.setConsumer(consumer);
    }

    @Override
    public void produce() throws DataSetException {
        try {
            this.delegate.produce();
        } finally {
            this.onClose.run();
        }
    }
}
