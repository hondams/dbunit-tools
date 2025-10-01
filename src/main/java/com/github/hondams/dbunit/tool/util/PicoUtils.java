package com.github.hondams.dbunit.tool.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class PicoUtils {

    public String[] getArgs(String line) {
        // TODO:シングルクォート、ダブルクォートの入れ子が正しき認識されない？
        // Windowsコマンドプロンプト
        //ダブルクォートで囲むと、スペースを含む文字列が1つの引数になる。
        //バックスラッシュ（\）は、ダブルクォート直前のみ特別扱いされ、ダブルクォート自体をエスケープできる。
        // シングルクォートは特別な意味を持たない。

        // Linux（Unix系シェル）
        // ダブルクォートで囲むと、スペースや多くの特殊文字を1つの引数として扱う。
        // バックスラッシュ（\）でダブルクォートや一部特殊文字をエスケープできる。
        // 主な特殊文字
        // $（変数展開）
        // `（コマンド置換）
        // "（ダブルクォート）
        // '（シングルクォート）
        // \（エスケープ）
        // *、?（ワイルドカード）
        // |（パイプ）
        // &（バックグラウンド実行）
        // ;（コマンド区切り）
        // (, )（サブシェル）
        // <, >（リダイレクト）

        String[] args = parseWindowsCmdArgs(line);
        log.info("line={}", line);
        log.info("args={}", Arrays.asList(args));
        return args;
    }

    // Windowsコマンドプロンプト
    //ダブルクォートで囲むと、スペースを含む文字列が1つの引数になる。
    //バックスラッシュ（\）は、ダブルクォート直前のみ特別扱いされ、ダブルクォート自体をエスケープできる。
    // シングルクォートは特別な意味を持たない。
    private String[] parseWindowsCmdArgs(String line) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        while (i < line.length()) {
            char c = line.charAt(i);
            if (inQuotes) {
                switch (c) {
                    case '"':
                        inQuotes = false;
                        i++;
                        break;
                    case '\\':
                        if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                            current.append('"');
                            i += 2;
                        } else {
                            current.append('\\');
                            i++;
                        }
                        break;
                    default:
                        current.append(c);
                        i++;
                        break;
                }
            } else {
                switch (c) {
                    case '"':
                        inQuotes = true;
                        i++;
                        break;
                    case ' ':
                        if (current.length() > 0) {
                            args.add(current.toString());
                            current.setLength(0);
                        }
                        i++;
                        break;
                    default:
                        current.append(c);
                        i++;
                        break;
                }

            }
        }
        if (current.length() > 0) {
            args.add(current.toString());
        }
        return args.toArray(new String[0]);
    }
}
