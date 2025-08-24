import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
class ClientHandler implements Runnable {
    private Socket socket;
    private static final Map<String, String> map = new ConcurrentHashMap<>();
    private static final Map<String, Deque<String>> rmap = new ConcurrentHashMap<>();
    static final ReentrantLock lock = new ReentrantLock();
private static final Map<String, ArrayDeque<CompletableFuture<String>>> blockedClients = new ConcurrentHashMap<>();
    
    static class Expiry {
        long timestamp;
        long durationMs;
        Expiry(long timestamp, long durationMs) {
            this.timestamp = timestamp;
            this.durationMs = durationMs;
        }
    }
    private Map<String, Expiry> time = new HashMap<>();
    
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
            int argVar = 0;
            while ((fromUser = in.readLine()) != null) {
                if (fromUser.startsWith("*") || fromUser.startsWith("$")) {
                    if (fromUser.startsWith("*")) {
                        argVar = Integer.parseInt(fromUser.substring(1)) - 1;
                    }
                    continue;
                }
                
                switch (fromUser.toUpperCase()) {
                    case "PING":
                        outputStream.write("+PONG\r\n".getBytes());
                        break;
                    case "ECHO":
                        handleEcho(in, outputStream);
                        break;
                    case "SET":
                        handleSet(in, outputStream, argVar);
                        break;
                    case "GET":
                        handleGet(in, outputStream);
                        break;
                    case "RPUSH":
                        handleRpush(in, outputStream, argVar);
                        break;
                    case "LPUSH":
                        handleLpush(in, outputStream, argVar);
                        break;
                    case "LRANGE":
                        handleLrange(in, outputStream);
                        break;
                    case "LLEN":
                        handleLlen(in, outputStream);
                        break;
                    case "LPOP":
                        handleLpop(in, outputStream, argVar);
                        break;
                    case "BLPOP":
                        handleBlpop(in, outputStream);
                        break;
                    default:
                        outputStream.write("-ERR unknown command\r\n".getBytes());
                }
                outputStream.flush();
            }
        } catch (Exception e) {
            System.out.println("Error handling client: " + e);
            e.printStackTrace();
        }
    }

    private void handleEcho(BufferedReader in, OutputStream outputStream) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue;
            }
            String resp = "+" + line + "\r\n";
            outputStream.write(resp.getBytes());
            break;
        }
    }

    private void handleSet(BufferedReader in, OutputStream outputStream, int argVar) throws IOException {
        String line;
        String key = null;
        String value = null;
        long duration = -1;
        boolean pxFound = false;
        
        while (argVar > 0 && (line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue;
            }
            
            if (key == null) {
                key = line;
                argVar--;
            } else if (value == null) {
                value = "$" + line.length() + "\r\n" + line + "\r\n";
                map.put(key, value);
                argVar--;
                
                if (argVar == 0) {
                    outputStream.write("+OK\r\n".getBytes());
                    break;
                }
            } else if (line.equalsIgnoreCase("px")) {
                pxFound = true;
                argVar--;
            } else if (pxFound && duration == -1) {
                try {
                    duration = Long.parseLong(line);
                    long now = System.currentTimeMillis();
                    time.put(key, new Expiry(now, duration));
                    outputStream.write("+OK\r\n".getBytes());
                    break;
                } catch (NumberFormatException e) {
                    outputStream.write("-ERR invalid timeout\r\n".getBytes());
                    break;
                }
            }
        }
    }

    private void handleGet(BufferedReader in, OutputStream outputStream) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue;
            }
            
            String value = map.getOrDefault(line, "$-1\r\n");
            if (!value.equals("$-1\r\n")) {
                Expiry ex = time.get(line);
                if (ex != null) {
                    long currentTime = System.currentTimeMillis();
                    if (currentTime > ex.timestamp + ex.durationMs) {
                        value = "$-1\r\n";
                        map.remove(line);
                        time.remove(line);
                    }
                }
            }
            outputStream.write(value.getBytes());
            break;
        }
    }
    private void handleLpush(BufferedReader in, OutputStream outputStream, int argVar) throws IOException {
        String line;
        String firstKey = null;
        int res = 0;
        
        lock.lock();
        try {
            while (argVar > 0 && (line = in.readLine()) != null) {
                if (line.startsWith("*") || line.startsWith("$")) {
                    continue;
                }
                
                if (firstKey == null) {
                    firstKey = line;
                    rmap.computeIfAbsent(firstKey, k -> new ArrayDeque<>());
                } else {
                    Deque<String> queue = rmap.get(firstKey);
                    queue.addFirst(line);
                    res = queue.size();
                }
                argVar--;
            }
            
            // Notify blocked clients if any
            if (firstKey != null) {
                ArrayDeque<CompletableFuture<String>> blockedQueue = blockedClients.get(firstKey);
                if (blockedQueue != null && !blockedQueue.isEmpty()) {
                    Deque<String> valueQueue = rmap.get(firstKey);
                    if (valueQueue != null && !valueQueue.isEmpty()) {
                        CompletableFuture<String> clientFuture = blockedQueue.pollFirst();
                        String element = valueQueue.pollFirst();
                        if (element != null && clientFuture != null) {
                            clientFuture.complete(element);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        
        outputStream.write((":" + res + "\r\n").getBytes());
    }

    private void handleLrange(BufferedReader in, OutputStream outputStream) throws IOException {
        String line;
        String key = null;
        String startStr = null;
        String endStr = null;
        
        // Read all parameters first
        int paramsRead = 0;
        while (paramsRead < 3 && (line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue; // Skip metadata lines
            }
            
            if (key == null) {
                key = line;
                paramsRead++;
            } else if (startStr == null) {
                startStr = line;
                paramsRead++;
            } else if (endStr == null) {
                endStr = line;
                paramsRead++;
            }
        }
        
        if (key == null || startStr == null || endStr == null) {
            outputStream.write("-ERR wrong number of arguments for LRANGE\r\n".getBytes());
            return;
        }
        
        int start, end;
        try {
            start = Integer.parseInt(startStr);
            end = Integer.parseInt(endStr);
        } catch (NumberFormatException e) {
            outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
            return;
        }
        
        lock.lock();
        try {
            Deque<String> queue = rmap.get(key);
            if (queue == null || queue.isEmpty()) {
                outputStream.write("*0\r\n".getBytes());
                return;
            }
            
            int size = queue.size();
            // Handle negative indices
            if (start < 0) start = size + start;
            if (end < 0) end = size + end;
            start = Math.max(0, start);
            end = Math.min(size - 1, end);
            
            if (start > end) {
                outputStream.write("*0\r\n".getBytes());
                return;
            }
            
            StringBuilder result = new StringBuilder();
            int count = end - start + 1;
            result.append("*").append(count).append("\r\n");
            
            Object[] elements = queue.toArray();
            for (int i = start; i <= end; i++) {
                String element = (String) elements[i];
                result.append("$").append(element.length()).append("\r\n");
                result.append(element).append("\r\n");
            }
            
            outputStream.write(result.toString().getBytes());
        } finally {
            lock.unlock();
        }
    }

    private void handleLlen(BufferedReader in, OutputStream outputStream) throws IOException {
        String line;
        
        lock.lock();
        try {
            while ((line = in.readLine()) != null) {
                if (line.startsWith("*") || line.startsWith("$")) {
                    continue;
                }
                
                Deque<String> queue = rmap.get(line);
                int len = (queue == null) ? 0 : queue.size();
                outputStream.write((":" + len + "\r\n").getBytes());
                break;
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleLpop(BufferedReader in, OutputStream outputStream, int argVar) throws IOException {
        String line;
        String key = null;
        int count = 1;
        
        lock.lock();
        try {
            while ((line = in.readLine()) != null) {
                if (line.startsWith("*") || line.startsWith("$")) {
                    continue;
                }
                
                if (key == null) {
                    key = line;
                    argVar--;
                    
                    if (argVar == 0) {
                        Deque<String> queue = rmap.get(key);
                        if (queue == null || queue.isEmpty()) {
                            outputStream.write("$-1\r\n".getBytes());
                            return;
                        }
                        
                        String value = queue.pollFirst();
                        outputStream.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
                        return;
                    }
                } else {
                    try {
                        count = Integer.parseInt(line);
                    } catch (NumberFormatException e) {
                        outputStream.write("-ERR invalid count\r\n".getBytes());
                        return;
                    }
                    
                    Deque<String> queue = rmap.get(key);
                    if (queue == null || queue.isEmpty()) {
                        outputStream.write("*0\r\n".getBytes());
                        return;
                    }
                    
                    count = Math.min(count, queue.size());
                    StringBuilder result = new StringBuilder();
                    result.append("*").append(count).append("\r\n");
                    
                    for (int i = 0; i < count; i++) {
                        String value = queue.pollFirst();
                        result.append("$").append(value.length()).append("\r\n");
                        result.append(value).append("\r\n");
                    }
                    
                    outputStream.write(result.toString().getBytes());
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private void handleBlpop(BufferedReader in, OutputStream outputStream) throws IOException {
        String line;
        String key = null;
        double timeout = 0;
        lock.lock();
        // Parse key and timeout
        while ((line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue;
            }

            if (key == null) {
                key = line;
            } else {
                try {
                    timeout = Double.parseDouble(line);
                    break;
                } catch (NumberFormatException e) {
                    outputStream.write("-ERR invalid timeout\r\n".getBytes());
                    outputStream.flush();
                    return;
                }
            }
        }
        
        CompletableFuture<String> future = new CompletableFuture<>();
        boolean immediateResponse = false;
        String immediateValue = null;

        
        try {
            Deque<String> queue = rmap.get(key);
            if (queue != null && !queue.isEmpty()) {
                immediateResponse = true;
                immediateValue = queue.pollFirst();  // serve immediately
            } else {
                // No element → block this client
                System.out.println("@@@BLPOP key "+key + " future "+future+" key exists "+blockedClients.containsKey(key));
                blockedClients.computeIfAbsent(key, k -> new ArrayDeque<>()).add(future);
            }
        } finally {
            lock.unlock();
        }

        if (immediateResponse) {
            try {
                outputStream.write(("*2\r\n$" + key.length() + "\r\n" + key +
                                    "\r\n$" + immediateValue.length() + "\r\n" +
                                    immediateValue + "\r\n").getBytes());
                outputStream.flush();
            } catch (IOException e) {
                // Client disconnected
            }
            return;
        }

        try {
            String value;
            if (timeout == 0) {
                System.out.println("@@@BLPOP future wait "+future);
                value = future.get(); // wait forever
            } else {
                value = future.get((long) (timeout * 1000), TimeUnit.MILLISECONDS);
            }

            outputStream.write(("*2\r\n$" + key.length() + "\r\n" + key +
                                "\r\n$" + value.length() + "\r\n" +
                                value + "\r\n").getBytes());
            outputStream.flush();
        } catch (TimeoutException e) {
            // Remove this future if still pending
            System.out.println("@@@BLPOP timeout");
            lock.lock();
            try {
                Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(key);
                if (blockedQueue != null) {
                    blockedQueue.remove(future);
                    if (blockedQueue.isEmpty()) {
                        blockedClients.remove(key);
                    }
                }
            } finally {
                lock.unlock();
            }

            try {
                outputStream.write("$-1\r\n".getBytes());  // ✅ correct RESP for null bulk string
                outputStream.flush();
            } catch (IOException ignored) {}
        } catch (Exception e) {
            // Cleanup on error
            System.out.println("@@@BLPOP Exception");
            lock.lock();
            try {
                Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(key);
                if (blockedQueue != null) {
                    blockedQueue.remove(future);
                    if (blockedQueue.isEmpty()) {
                        blockedClients.remove(key);
                    }
                }
            } finally {
                lock.unlock();
            }

            try {
                outputStream.write("$-1\r\n".getBytes());
                outputStream.flush();
            } catch (IOException ignored) {}
        }
    }

    private void handleRpush(BufferedReader in, OutputStream outputStream, int argVar) throws IOException {
        String line;
        String firstKey = null;
        int res = 0;
        List<String> values = new ArrayList<>();
        lock.lock();
        // Parse all values
        while (argVar > 0 && (line = in.readLine()) != null) {
            if (line.startsWith("*") || line.startsWith("$")) {
                continue;
            }

            if (firstKey == null) {
                firstKey = line;
            } else {
                values.add(line);
            }
            argVar--;
        }

       
        try {
            Deque<String> queue = rmap.computeIfAbsent(firstKey, k -> new ArrayDeque<>());
            for (String value : values) {
                queue.addLast(value);
            }
            res = queue.size();  // ✅ final size of the list (no adjustment!)
            // Notify blocked clients if any
            Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(firstKey);
            
            if (blockedQueue == null) {
                System.out.println("@@@RPUSH blockedQueue is null");
            }
            if (blockedQueue != null && blockedQueue.isEmpty()) {
                System.out.println("@@@RPUSH blockedQueue is empty");
            }
            if (queue.isEmpty()) {
                System.out.println("@@@ RPUSH queue is empty ");
            }
            if (blockedQueue != null && !blockedQueue.isEmpty() && !queue.isEmpty()) {
                CompletableFuture<String> clientFuture = blockedQueue.pollFirst();
                String element = queue.pollFirst();  // give the earliest element
                System.out.println("@@@ RPUSH element "+element+" future "+clientFuture);
                if (clientFuture != null && element != null) {
                    
                    clientFuture.complete(element);
                    System.out.println("@@@ RPUSH complete future "+clientFuture);
                }
            }
            if (blockedQueue != null && blockedQueue.isEmpty()) {
                blockedClients.remove(firstKey);
            }

            
        } finally {
            lock.unlock();
        }

        try {
            outputStream.write((":" + res + "\r\n").getBytes());
            outputStream.flush();
        } catch (IOException ignored) {}
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
