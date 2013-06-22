package com.github.bluetiger9.nosql.benchmarking.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class ExportTools {
    public static void exportTimeSeries(List<TimeMeasurment> data, File file) throws IOException {
        final PrintWriter writer = new PrintWriter(new FileWriter(file, true));
        for (Object o : data) {
            writer.println(o.toString());
        }
        writer.close();
    }
}
