package com.github.hondams.dbunit.tool.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TableWriter implements Closeable {

    private final LineWriter writer;

    private List<String> headerNames;
    private List<PrintLineAlignment> alignments;
    private int[] displaySizes;
    boolean writtenHeader;

    public void setHeaderNames(List<String> headerNames) {
        if (this.writtenHeader) {
            throw new IllegalStateException("Header already written");
        }
        if (this.headerNames != null) {
            throw new IllegalStateException("Header names already set");
        }
        if (this.alignments != null && this.alignments.size() != headerNames.size()) {
            throw new IllegalStateException(
                "Header names size and alignments size are different: " + headerNames.size()
                    + " != " + this.alignments.size());
        }
        if (this.displaySizes != null && this.displaySizes.length != headerNames.size()) {
            throw new IllegalStateException(
                "Header names size and displaySizes size are different: " + headerNames.size()
                    + " != " + this.displaySizes.length);
        }

        this.headerNames = headerNames;
        if (this.displaySizes == null) {
            this.displaySizes = createDefaultMaxLengths();
        }
        if (this.alignments == null) {
            this.alignments = createDefaultAlignments();
        }
    }

    public void setAlignments(List<PrintLineAlignment> alignments) {
        if (this.writtenHeader) {
            throw new IllegalStateException("Header already written");
        }
        if (this.headerNames != null && this.headerNames.size() != alignments.size()) {
            throw new IllegalStateException(
                "Header names size and alignments size are different: " + this.headerNames.size()
                    + " != " + alignments.size());
        }
        this.alignments = alignments;
    }

    public void setDisplaySizes(List<Integer> displaySizes) {
        if (this.writtenHeader) {
            throw new IllegalStateException("Header already written");
        }
        if (this.headerNames != null && this.headerNames.size() != displaySizes.size()) {
            throw new IllegalStateException(
                "Header names size and displaySizes size are different: " + this.headerNames.size()
                    + " != " + displaySizes.size());
        }
        this.displaySizes = displaySizes.stream().mapToInt(Integer::intValue).toArray();
    }

    private int[] createDefaultMaxLengths() {
        int[] maxLengths = new int[this.headerNames.size()];
        for (int i = 0; i < this.headerNames.size(); i++) {
            maxLengths[i] = this.headerNames.get(i).length();
        }
        return maxLengths;
    }

    private List<PrintLineAlignment> createDefaultAlignments() {
        List<PrintLineAlignment> alignments = new ArrayList<>();
        for (int i = 0; i < this.headerNames.size(); i++) {
            alignments.add(PrintLineAlignment.LEFT);
        }
        return alignments;
    }

    private void writeHeaderLine() throws IOException {
        StringBuilder headerLine = new StringBuilder();
        for (int i = 0; i < this.headerNames.size(); i++) {
            String header = this.headerNames.get(i);
            int displaySize = this.displaySizes[i];
            headerLine.append(ConsoleUtils.fillRight(header, displaySize));
            headerLine.append(" | ");
        }
        headerLine.setLength(headerLine.length() - " | ".length());
        this.writer.println(headerLine.toString());
    }

    private void writeSeparatorLine() throws IOException {
        StringBuilder separatorLine = new StringBuilder();
        for (int displaySize : this.displaySizes) {
            separatorLine.append("-".repeat(displaySize));
            separatorLine.append("-+-");
        }
        separatorLine.setLength(separatorLine.length() - "-+-".length());
        this.writer.println(separatorLine.toString());
    }

    public void writeRow(List<?> row) throws IOException {
        if (this.headerNames == null) {
            throw new IllegalStateException("Header names not set");
        }
        if (this.alignments == null) {
            throw new IllegalStateException("Alignments not set");
        }
        if (this.displaySizes == null) {
            throw new IllegalStateException("Display sizes not set");
        }
        if (row.size() != this.headerNames.size()) {
            throw new IllegalArgumentException(
                "Row size and header names size are different: " + row.size() + " != "
                    + this.headerNames.size());
        }
        if (!this.writtenHeader) {
            writeHeaderLine();
            writeSeparatorLine();
            this.writtenHeader = true;
        }

        writeRowLine(row);
    }

    private void writeRowLine(List<?> row) throws IOException {
        StringBuilder rowLine = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            String cell = toStringValue(row.get(i));
            int displaySize = this.displaySizes[i];
            PrintLineAlignment alignment = this.alignments.get(i);
            String formattedCell;
            switch (alignment) {
                case LEFT:
                    formattedCell = ConsoleUtils.fillRight(cell, displaySize);
                    break;
                case RIGHT:
                    formattedCell = ConsoleUtils.fillLeft(cell, displaySize);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown alignment: " + alignment);
            }
            rowLine.append(formattedCell);
            rowLine.append(" | ");
        }
        rowLine.setLength(rowLine.length() - " | ".length());
        this.writer.println(rowLine.toString());
    }

    private String toStringValue(Object v) {
        if (v == null) {
            return "<null>";
        }
        return v.toString();
    }

    @Override
    public void close() throws IOException {
        if (!this.writtenHeader) {
            writeHeaderLine();
            writeSeparatorLine();
            this.writtenHeader = true;
        }
        this.writer.close();
    }
}
