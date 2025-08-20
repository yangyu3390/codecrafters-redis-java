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
        else if (fromUser.equalsIgnoreCase("ECHO")) {
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            String resp = "+" + fromUser + "\r\n";
            outputStream.write(resp.getBytes());
            outputStream.flush();
            break;
          }
        }
        else if (fromUser.equalsIgnoreCase("SET")) {
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
              value = "$" + Integer.toString(fromUser.length()) + "\r\n" + fromUser + "\r\n";
              map.put(key, value);
              System.out.println("!!!set value "+ value);
              System.out.println("!!!get "+ map.get(key));
              String resp = "+OK\r\n";
              outputStream.write(resp.getBytes());
              outputStream.flush();
              break;
            }
          }
        }
        else if (fromUser.equalsIgnoreCase("GET")) {
          String value;
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            value = map.getOrDefault(fromUser, "$-1\r\n");
            outputStream.write(value.getBytes());
            outputStream.flush();
            break;
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
