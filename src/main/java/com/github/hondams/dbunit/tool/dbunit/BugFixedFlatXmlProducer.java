package com.github.hondams.dbunit.tool.dbunit;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

public class BugFixedFlatXmlProducer extends FlatXmlProducer {

    public BugFixedFlatXmlProducer(InputSource xmlSource) {
        super(xmlSource);
    }

    public BugFixedFlatXmlProducer(InputSource xmlSource, boolean dtdMetadata) {
        super(xmlSource, dtdMetadata);
    }

    public BugFixedFlatXmlProducer(InputSource xmlSource, IDataSet metaDataSet) {
        super(xmlSource, metaDataSet);
    }

    public BugFixedFlatXmlProducer(InputSource xmlSource, EntityResolver resolver) {
        super(xmlSource, resolver);
    }

    public BugFixedFlatXmlProducer(InputSource xmlSource, boolean dtdMetadata,
        boolean columnSensing) {
        super(xmlSource, dtdMetadata, columnSensing);
    }

    public BugFixedFlatXmlProducer(InputSource xmlSource, boolean dtdMetadata,
        boolean columnSensing, boolean caseSensitiveTableNames) {
        super(xmlSource, dtdMetadata, columnSensing, caseSensitiveTableNames);
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        super.setConsumer(new BugFixedDataSetConsumer(consumer));
    }
}
