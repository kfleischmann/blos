package eu.blos.java.stratosphere.sketchbuilder;


import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;

public class SketchPlan implements Program {

    @Override
    public Plan getPlan(String... args) {
        FileDataSource source = new FileDataSource(new TextInputFormat(), "file:///path/to/input");


        // Operations on the data set go here
        // ...

        FileDataSink sink = new FileDataSink(new CsvOutputFormat(), "file:///path/to/result");

        return new Plan(sink);
    }


    public static void main(String[] args) throws Exception {
    }
}