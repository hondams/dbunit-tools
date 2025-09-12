package com.github.hondams.dbunit.tool.command;

import com.github.hondams.dbunit.tool.dbunit.DbUnitFileFormat;
import com.github.hondams.dbunit.tool.dbunit.DbUnitUtils;
import com.github.hondams.dbunit.tool.util.ConsolePrinter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.dbunit.dataset.IDataSet;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "convert", description = "Convert or merge data file format")
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class ConvertCommand implements Callable<Integer> {

    @Option(names = {"-i", "--input"}, split = ",", required = true)
    String[] input;

    @Option(names = {"-f", "--format"})
    DbUnitFileFormat format;

    @Option(names = {"-o", "--output"}, required = true)
    String output;

    @Option(names = {"-m", "--output-mode"})
    String outputMode;

    @Option(names = {"-l", "--limit"})
    int limit = -1;

    @Override
    public Integer call() throws Exception {

        // Inputファイルを検索する。
        // ※検知したファイルリストを、画面・ログに表示
        // tempディレクトリを作る
        // DBのメタデータ（dbdef exportの結果）を取得する。
        // ファイル単位に読込、テーブル単位に分割して、レコードをソートしたファイルをtempディレクトリに保存する。
        // ※ 1テーブルで、5GBとかある場合もあるので、一定件数で分割して、出力する。結局、後続処理で、マージソートでマージすることを期待。
        //　DBのメタデータ（dbdef exportの結果）から、ITableのメタデータを作る。
        // テーブル単位のファイルを複数同時に開き、マージソートで、マージしながら、テーブル単位の一時ファイルをtempディレクトリに保存する。
        // テーブル単位の一時ファイルをまとめて、IDataSetにまとめる。
        // ※ オプションんにより、レコード数でファイルを分けたり、テーブルごとに分けたりする。
        // IDataSetを出力する。
        // キーが同じレコードは、優先し、後のファイルのレコードで上書し、画面・ログに表示

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
