package ca.concordia.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import ca.concordia.filesystem.FileSystemManager;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final FileSystemManager fsManager;
    
    public ClientHandler(Socket clientSocket, FileSystemManager fsManager) {
        this.clientSocket = clientSocket;
        this.fsManager = fsManager;
    }

    @Override
    public void run() {
        System.out.println("Handling client in thread: " + Thread.currentThread().getName());

    try (
        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
    ) {
                    String line;
                    while ((line = reader.readLine()) != null) {

                        System.out.println("Received from client: " + line);
                        String[] parts = line.trim().split(" ",3);
                        String command = parts[0].toUpperCase();
                        try{
                        switch (command) {

                            case "CREATE":
                            if(parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }   
                                fsManager.lockWrite();
                                try {
                                    fsManager.createFile(parts[1]);
                                    writer.println("SUCCESS: File '" + parts[1] + "' created.");
                                    writer.flush();
                                } finally {
                                    fsManager.unlockWrite();
                                }
                                break;
                            case "WRITE":
                            if(parts.length < 3) {
                                writer.println("ERROR: Filename and content required.");
                                break;
                            }   
                                fsManager.lockWrite();
                                try {
                                    fsManager.writeFile(parts[1], parts[2].getBytes());
                                    writer.println("SUCCESS: Written to file '" + parts[1] + "'.");
                                    writer.flush();
                                } finally {
                                    fsManager.unlockWrite();
                                }
                                break;
                            case "READ":
                            if(parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }   
                                fsManager.lockRead();
                                try {
                                    byte[] data = fsManager.readFile(parts[1]);
                                    writer.println("SUCCESS: Read from file '" + parts[1] + "': " + new String(data));
                                    writer.flush();
                                } finally {
                                    fsManager.unlockRead();
                                }
                                break;
                            case "DELETE":
                            if(parts.length < 2) {
                                writer.println("ERROR: Filename required.");
                                break;
                            }   
                                fsManager.lockWrite();
                                try {
                                    fsManager.deleteFile(parts[1]);
                                    writer.println("SUCCESS: File '" + parts[1] + "' deleted.");
                                    writer.flush();
                                } finally {
                                    fsManager.unlockWrite();
                                }
                                break;
                            case "LIST":
                                fsManager.lockRead();
                                try {
                                    String[] files = fsManager.listFiles();
                                    StringBuilder response = new StringBuilder("SUCCESS: Files:");
                                    for (String file : files) {
                                        response.append(" ").append(file);
                                    }
                                    writer.println(response.toString());
                                    writer.flush();
                                } finally {
                                    fsManager.unlockRead();
                                }
                                break;
                            
                            case "QUIT":
                                writer.println("SUCCESS: Disconnecting.");
                                return;
                            default:
                                writer.println("ERROR: Unknown command.");
                                break;
                        }
                        } catch (Exception e) {
                            writer.println("ERROR: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        clientSocket.close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }
        }