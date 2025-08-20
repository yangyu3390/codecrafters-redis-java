import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

class ClientHandler implements Runnable {
  private Socket socket;
  private Map<String, String> map = new HashMap<>();
  public ClientHandler(Socket socket) {
    this.socket = socket;
  }
  @Override
  public void run() {    
    try (
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      OutputStream outputStream = socket.getOutputStream();
    ) {
      String fromUser;     
      while ((fromUser=in.readLine())!=null) {
        if (fromUser.equalsIgnoreCase("PING")) {
          outputStream.write("+PONG\r\n".getBytes());
        }
        if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
          // This is RESP metadata, ignore it
          continue;
        }
        if (fromUser.equalsIgnoreCase("ECHO")) {
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            String resp = "+" + fromUser + "\r\n";
            outputStream.write(resp.getBytes());
          }
        }
        if (fromUser.equalsIgnoreCase("SET")) {
          boolean keyFound = false;
          String key = null;
          String value;
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (!keyFound) {
              keyFound = true;
              key = fromUser;
            } else {
              value = fromUser;
              map.put(key, value);
              String resp = "+OK\r\n";
              outputStream.write(resp.getBytes());
            }
          }
        }
        if (fromUser.equalsIgnoreCase("GET")) {
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            String value = map.getOrDefault(fromUser, "$-1\r\n");
            String resp = "$" + Integer.toString(value.length()) + "\r\n" + value + "\r\n";
            outputStream.write(resp.getBytes());
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
       ServerSocket serverSocket = null;
       Socket clientSocket = null;
       int port = 6379;
       try {
         serverSocket = new ServerSocket(port);
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         // Wait for connection from client.
        
        while (true) {
          clientSocket = serverSocket.accept();
          Thread clientThread = new Thread(new ClientHandler(clientSocket));
          clientThread.start();
        }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       } finally {
         try {
           if (clientSocket != null) {
             clientSocket.close();
           }
         } catch (IOException e) {
           System.out.println("IOException: " + e.getMessage());
         }
       }
  }
}
