package com.github.hondams.dbunit.tool.dbunit;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.XmlProducer;
import org.xml.sax.InputSource;

public class BugFixedXmlProducer extends XmlProducer {

    public BugFixedXmlProducer(InputSource xmlSource) {
        super(xmlSource);
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        super.setConsumer(new BugFixedDataSetConsumer(consumer));
    }
}
