package com.kugou.test;

/**
 * Created by jaykelin on 2016/11/2.
 */
public class Test {

    public static void main(String[] args){
        String line = "sweef|wlk违反了违法|sdfwef|lslls\tsss算了";
        String replaceChar = " ";
        String fieldsTerminatedBy = "|";
        String nullNonString = " ";
        line = line.replace('\t', replaceChar.charAt(0))
                .replace(fieldsTerminatedBy.charAt(0), '\t')
                .replace("\\N", nullNonString);
        System.out.println(line);
    }
}
