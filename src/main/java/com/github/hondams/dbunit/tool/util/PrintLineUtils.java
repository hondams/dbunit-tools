package com.github.hondams.dbunit.tool.util;

import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class PrintLineUtils {

    public List<String> getTableLines(String prefix, List<String> headers,
        List<PrintLineAlignment> alignments, List<List<String>> rows) {
        if (headers.isEmpty()) {
            throw new IllegalArgumentException("headers.isEmpty()");
        }
        if (headers.size() != alignments.size()) {
            throw new IllegalArgumentException("headers.size() != alignments.size()");
        }

        int[] maxLengthArray = new int[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            maxLengthArray[i] = headers.get(i).length();
        }
        int rowIndex = 0;
        for (List<String> row : rows) {
            for (int i = 0; i < row.size() && i < maxLengthArray.length; i++) {
                String cell = row.get(i);
                if (cell.length() > maxLengthArray[i]) {
                    maxLengthArray[i] = cell.length();
                }
            }
            if (row.size() < headers.size()) {
                log.warn("row.size() < headers.size(): [{}]({})={}", rowIndex, row.size(), row);
            }
            if (headers.size() < row.size()) {
                log.warn("headers.size() < row.size(): [{}]({})={}", rowIndex, row.size(), row);
            }
            rowIndex++;
        }

        List<String> lines = new ArrayList<>();
        lines.add(prefix + buildHeaderLine(headers, maxLengthArray));
        lines.add(prefix + buildHeaderLine(maxLengthArray));
        for (List<String> row : rows) {
            lines.add(prefix + buildRowLine(row, maxLengthArray, alignments));
        }
        return lines;
    }

    private String buildHeaderLine(List<String> headers, int[] maxLengthArray) {
        StringBuilder headerLine = new StringBuilder();
        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i);
            int maxLength = maxLengthArray[i];
            headerLine.append(String.format("%-" + maxLength + "s", header));
            headerLine.append(" | ");
        }
        headerLine.setLength(headerLine.length() - " | ".length());
        return headerLine.toString();
    }


    private String buildRowLine(List<String> headers, int[] maxLengthArray,
        List<PrintLineAlignment> alignments) {
        StringBuilder headerLine = new StringBuilder();
        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i);
            int maxLength = maxLengthArray[i];
            PrintLineAlignment alignment = alignments.get(i);
            String formattedHeader;
            switch (alignment) {
                case LEFT:
                    formattedHeader = String.format("%-" + maxLength + "s", header);
                    break;
                case RIGHT:
                    formattedHeader = String.format("%" + maxLength + "s", header);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown alignment: " + alignment);
            }
            headerLine.append(formattedHeader);
            headerLine.append(" | ");
        }
        headerLine.setLength(headerLine.length() - " | ".length());
        return headerLine.toString();
    }

    private String buildHeaderLine(int[] maxLengthArray) {
        StringBuilder separatorLine = new StringBuilder();
        for (int maxLength : maxLengthArray) {
            separatorLine.append("-".repeat(maxLength));
            separatorLine.append("-+-");
        }
        separatorLine.setLength(separatorLine.length() - "-+-".length());
        return separatorLine.toString();
    }
}