package org.example.app;

import org.example.app.dto.DataImpl;
import org.gosspy.Gosspy;

import java.io.IOException;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        Gosspy gosspy =  new Gosspy();
        gosspy.run(new DataImpl());
    }
}