import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.ArrayList;
import java.util.Deque;

class ClientHandler implements Runnable {
  private Socket socket;
  private static final Map<String, String> map = new ConcurrentHashMap<>();
  private static final Map<String, Stack<String>> rmap = new ConcurrentHashMap<>();
  static final ReentrantLock lock = new ReentrantLock();
  static final Condition notEmpty = lock.newCondition();
  public ClientHandler(Socket socket) {
    this.socket = socket;
  }

  static class Expiry {
      long timestamp;
      long durationMs;
      Expiry(long timestamp, long durationMs) {
          this.timestamp = timestamp;
          this.durationMs = durationMs;
      }
  }
  private Map<String, Expiry> time = new HashMap<>();
  @Override
  public void run() {    
    try (
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      OutputStream outputStream = socket.getOutputStream();
    ) {
      String fromUser;     
      int argVar = 0;
      while ((fromUser=in.readLine())!=null) {
        
        if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
          // This is RESP metadata, ignore it
          if (fromUser.startsWith("*")) {
            // minus 1 for the SET command
            argVar = Integer.parseInt(fromUser.substring(1))-1;
          }
          continue;
        } else if (fromUser.equalsIgnoreCase("PING")) {
          outputStream.write("+PONG\r\n".getBytes());
        } else if (fromUser.equalsIgnoreCase("ECHO")) {
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
        } else if (fromUser.equalsIgnoreCase("SET")) {
          boolean keyFound = false;
          boolean valueFound = false;
          boolean pxFound = false;
          String key = null;
          String value;
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (!keyFound) {
              argVar -= 1;
              keyFound = true;
              key = fromUser;
            } else if (!valueFound){
              argVar -= 1;
              valueFound = true;
              value = "$" + Integer.toString(fromUser.length()) + "\r\n" + fromUser + "\r\n";
              map.put(key, value);
              // System.out.println("!!!set value "+ value);
              // System.out.println("!!!get "+ map.get(key));
              String resp = "+OK\r\n";
              outputStream.write(resp.getBytes());
              outputStream.flush();
              
              if (argVar == 0) {
                break;
              }
            } else if (!pxFound && fromUser.equalsIgnoreCase("px")) {
              pxFound = true;
              argVar -= 1;
            } else if (pxFound && argVar==1) {
              argVar -= 1;
              long duration = Integer.parseInt(fromUser.substring(0));
              long now = System.currentTimeMillis();
              time.put(key, new Expiry(now, duration));
              break;
            }
          }
        } else if (fromUser.equalsIgnoreCase("GET")) {
          String value;
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            value = map.getOrDefault(fromUser, "$-1\r\n");
            if (!value.equals("$-1\r\n")) {
              Expiry ex = time.get(fromUser);
              if (ex != null) {
                long timestamp = ex.timestamp;
                long durationMs = ex.durationMs;
                long currentTime = System.currentTimeMillis();
                if (currentTime > timestamp + durationMs) {
                    System.out.println("Expired");
                    value = "$-1\r\n";
                } else {
                    System.out.println("Still valid");
                }
              }
            }
            outputStream.write(value.getBytes());
            outputStream.flush();
            break;
          }
        } else if (fromUser.equalsIgnoreCase("RPUSH")) {
          String firstKey = null;
          lock.lock();
          
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (firstKey == null) {
              firstKey = fromUser;
              if (!rmap.containsKey(firstKey)) {
                rmap.put(fromUser, new Stack<String>());
              } 
            } else {
              Stack<String> val = rmap.get(firstKey);
              val.addLast(fromUser);
            }
            argVar -= 1;
            if(argVar == 0) {
              String res = ":" + Integer.toString(rmap.get(firstKey).size())+"\r\n";
              outputStream.write(res.getBytes());
              outputStream.flush();
              try {
                notEmpty.signalAll(); // wake up all waiting threads
              } finally {
                  lock.unlock();
              }
              break;
            }
          }
        } else if (fromUser.equalsIgnoreCase("LPUSH")) {
          String firstKey = null;
          lock.lock();
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (firstKey == null) {
              firstKey = fromUser;
              if (!rmap.containsKey(firstKey)) {
                rmap.put(fromUser, new Stack<String>());
              } 
            } else {
              Stack<String> val = rmap.get(firstKey);
              val.addFirst(fromUser);
            }
            argVar -= 1;
            if(argVar == 0) {
              String res = ":" + Integer.toString(rmap.get(firstKey).size())+"\r\n";
              outputStream.write(res.getBytes());
              outputStream.flush();
              try {
                notEmpty.signalAll(); // wake up all waiting threads
              } finally {
                  lock.unlock();
              }
              break;
            }
          }
        } else if (fromUser.equalsIgnoreCase("LRANGE")) {
          String empty = "*0\r\n";
          String startIdx = null;
          String endIdx = null;
          String key = null;
          int listLen = 0;
          lock.lock();
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            
            if (key == null) {
              key = fromUser;
              if (!rmap.containsKey(fromUser)) {
                outputStream.write(empty.getBytes());
                outputStream.flush();
                break;
              }
              listLen = rmap.get(key).size();
            } else if (startIdx == null) {
              startIdx = fromUser;
              if (Integer.parseInt(startIdx) < 0) {
                if (-Integer.parseInt(startIdx)>=listLen) {
                  startIdx = "0";
                } else {
                  int len = Integer.parseInt(startIdx) + listLen;
                  startIdx = Integer.toString(len);
                }
              }
              if (Integer.parseInt(startIdx)>=listLen) {
                outputStream.write(empty.getBytes());
                outputStream.flush();
                
                lock.unlock();
                
                break;
              }
              
            } else if (endIdx == null) {
              endIdx = fromUser;
              if (Integer.parseInt(endIdx) < 0) {
                if (-Integer.parseInt(endIdx)>=listLen) {
                  endIdx = "0";
                } else {
                  int len = Integer.parseInt(endIdx) + listLen;
                  endIdx = Integer.toString(len);
                }
              }
              if (Integer.parseInt(endIdx)>=listLen) {
                endIdx = Integer.toString(listLen-1);
              } 
              if (Integer.parseInt(startIdx) > Integer.parseInt(endIdx)) {
                outputStream.write(empty.getBytes());
                outputStream.flush();
                
                lock.unlock();
                
                break;
              }
              int startIntIdx = Integer.parseInt(startIdx);
              int endIntIdx = Integer.parseInt(endIdx);
              StringBuilder res = new StringBuilder();
              int len = endIntIdx - startIntIdx + 1;
              res.append("*"+len+"\r\n");
              for(int i = startIntIdx; i <= endIntIdx; i++) {
                String val = rmap.get(key).get(i);
                int valLen = val.length();
                res.append("$"+valLen+"\r\n");
                res.append(val+"\r\n");
              }
              outputStream.write(res.toString().getBytes());
              outputStream.flush();
              
              lock.unlock();
              
              break;
            }
          }
        } else if (fromUser.equalsIgnoreCase("LLEN")) {
          lock.lock();
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
           
            if (!rmap.containsKey(fromUser)) {
              outputStream.write(":0\r\n".getBytes());
              outputStream.flush();
              
              lock.unlock();
              
              break;
            } else {
              int len = rmap.get(fromUser).size();
              outputStream.write((":" + len + "\r\n").getBytes());
              outputStream.flush();
              lock.unlock();
              break;
            } 
          }
        } else if (fromUser.equalsIgnoreCase("LPOP")) {
          String key = null;
          String len = null;
          lock.lock();
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (key == null) {
              key = fromUser;
            
              if (!rmap.containsKey(key) || rmap.get(key).size()==0) {
                outputStream.write("$-1\r\n".getBytes());
                outputStream.flush();
                lock.unlock();
                break;
              } 
              argVar -= 1;
              if (argVar == 0) {
                String val = rmap.get(key).getFirst();
                rmap.get(key).removeFirst();
                outputStream.write(("$"+val.length()+"\r\n"+val+"\r\n").getBytes());
                outputStream.flush();
                lock.unlock();
                break;
              }
            } else if (key != null && len == null) {
              len = fromUser;
              int intLen = Integer.parseInt(len);
              intLen = Math.min(intLen, rmap.get(key).size());
              StringBuilder res = new StringBuilder();
              res.append("*"+intLen+"\r\n");
              while (intLen > 0) {
                String val = rmap.get(key).getFirst();
                rmap.get(key).removeFirst();
                res.append("$"+val.length()+"\r\n");
                res.append(val+"\r\n");
                intLen -= 1;
              }
              outputStream.write(res.toString().getBytes());
              outputStream.flush();
              lock.unlock();
              break;
            }
          }
        } else if (fromUser.equalsIgnoreCase("BLPOP")) {
          String key = null;
          String timeout = null;
          lock.lock();
          while ((fromUser=in.readLine())!=null) {
            if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
              // This is RESP metadata, ignore it
              continue;
            }
            if (key == null) {
              key = fromUser;
              // if (!rmap.containsKey(key) || rmap.get(key).size()==0) {
              //   outputStream.write("$-1\r\n".getBytes());
              //   outputStream.flush();
              //   break;
              // } 
            } else if (key != null && timeout == null) {
              timeout = fromUser;
              double doubleTimeout = Double.parseDouble(timeout);
              while (!rmap.containsKey(key)){
                if (doubleTimeout-0.0 < 0.0000001) {
                  try {
                    notEmpty.await();
                  } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  } // 
                } else {
                  try {
                    notEmpty.awaitNanos((long)(doubleTimeout * 1_000_000_000L));
                  } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  }
                }
              }
              
              String res = rmap.get(key).pop();
              int resLen = res.length();
              
              outputStream.write(("*2\r\n"+"$"+key.length()+"\r\n"+key+"\r\n"+"$"+resLen+"\r\n"+res+"\r\n").getBytes());
              outputStream.flush();
              break;
            }
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
