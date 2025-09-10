package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.IDataSet;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "convert", description = "Convert or merge data file format")
@Component
@Slf4j
public class ConvertCommand implements Callable<Integer> {

    @Option(names = {"-i", "--input"}, split = ",", required = true)
    String[] input;

    @Option(names = {"-f", "--format"})
    String format;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Override
    public Integer call() throws Exception {

        List<IDataSet> dataSets = new ArrayList<>();
        for (String in : this.input) {
            File f = new File(in);
            dataSets.add(DbUnitUtils.load(f));
        }

        IDataSet mergedDataSet = DbUnitUtils.merge(dataSets);
        File outputFile = new File(this.output);

        if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
            boolean created = outputFile.getParentFile().mkdirs();
            if (!created) {
                throw new IllegalStateException(
                    "Failed to create directories: " + outputFile.getParentFile());
            }
        }
        DbUnitUtils.save(mergedDataSet, outputFile, this.format);
        if (dataSets.size() == 1) {
            ConsolePrinter.println(log, "Converted to " + outputFile.getAbsolutePath());
        } else {
            ConsolePrinter.println(log, "Merged to " + outputFile.getAbsolutePath());
        }
        return 0;
    }
}
