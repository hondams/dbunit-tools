package com.github.hondams.dbunit.tool.dbunit;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import org.dbunit.util.xml.XmlWriter;

public class BugFixedXmlWriter extends XmlWriter {

    public BugFixedXmlWriter(Writer writer) {
        super(writer);
    }

    public BugFixedXmlWriter(Writer writer, String encoding) {
        super(writer, encoding);
    }

    public BugFixedXmlWriter(OutputStream outputStream, String encoding)
        throws UnsupportedEncodingException {
        super(outputStream, encoding);
    }

    @Override
    protected String convertCharacterToEntity(char currentChar, boolean literally) {
        String entity = null;
        switch (currentChar) {
            case '\t':
                entity = "&#09;";
                break;
            case '\n':
                if (literally) {
                    entity = "&#xA;";
                }
                break;
            case '\r':
                if (literally) {
                    entity = "&#xD;";
                }
                break;
            case '&':
                entity = "&amp;";
                break;
            case '<':
                entity = "&lt;";
                break;
            case '>':
                entity = "&gt;";
                break;
            case '\"':
                entity = "&quot;";
                break;
            case '\'':
                entity = "&apos;";
                break;
            case '\u0000':
                entity = "&amp;#x0;";
                break;
            default:
                // 不要なサロゲートペア処理を削除
                //                if ((currentChar > 0x7f) && !isValidXmlChar(currentChar)) {
                //                    entity = "&#" + String.valueOf((int) currentChar) + ";";
                //                }
                break;
        }
        return entity;
    }
}
