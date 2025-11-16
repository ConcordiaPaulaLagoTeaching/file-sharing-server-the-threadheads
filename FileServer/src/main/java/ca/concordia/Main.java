package ca.concordia;

import java.io.IOException;

import ca.concordia.server.FileServer;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello and welcome!");

        FileServer server = new FileServer(12345, "filesystem.dat", 10 * 128);
        // Start the file server
        server.start();
    }
}