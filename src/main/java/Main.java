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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
class ClientHandler implements Runnable {
    private Socket socket;
    private static final Map<String, String> map = new ConcurrentHashMap<>();
    private static final Map<String, Deque<String>> rmap = new ConcurrentHashMap<>();
    private static final Map<String, Deque<Condition>> conditions = new HashMap<>();
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
            String line;
            while ((line = in.readLine()) != null) {
                // First, check if it's a RESP array (the usual case for commands)
                if (line.startsWith("*")) {
                    int numArgs = Integer.parseInt(line.substring(1));
                    
                    // If numArgs is 0, it's an empty array, which is an error in this context
                    if (numArgs == 0) {
                        outputStream.write("-ERR invalid command format\r\n".getBytes());
                        outputStream.flush();
                        continue;
                    }
                    
                    // Read the command name
                    // The next line will be '$' followed by length, then the command name
                    if (!in.readLine().startsWith("$")) {
                        // Malformed protocol
                        outputStream.write("-ERR invalid command format\r\n".getBytes());
                        outputStream.flush();
                        continue;
                    }
                    String command = in.readLine().toUpperCase();
                    
                    // Read the arguments
                    List<String> args = new ArrayList<>();
                    for (int i = 0; i < numArgs - 1; i++) {
                        if (!in.readLine().startsWith("$")) {
                            // Malformed protocol
                            outputStream.write("-ERR invalid command format\r\n".getBytes());
                            outputStream.flush();
                            // Clear the input buffer to prevent cascading errors
                            while (in.ready()) in.read();
                            break;
                        }
                        args.add(in.readLine());
                    }

                    // Now, with the full command and arguments parsed, call the appropriate handler
                    switch (command) {
                        case "PING":
                            // PING takes no arguments, so we can ignore the 'args' list
                            outputStream.write("+PONG\r\n".getBytes());
                            break;
                        case "ECHO":
                            // Now the handler receives the pre-parsed arguments
                            handleEcho(outputStream, args);
                            break;
                        case "SET":
                            handleSet(outputStream, args);
                            break;
                        case "GET":
                            handleGet(outputStream, args);
                            break;
                        case "RPUSH":
                            handleRpush(outputStream, args);
                            break;
                        case "LPUSH":
                            handleLpush(outputStream, args);
                            break;
                        case "LRANGE":
                            handleLrange(outputStream, args);
                            break;
                        case "LLEN":
                            handleLlen(outputStream, args);
                            break;
                        case "LPOP":
                            handleLpop(outputStream, args);
                            break;
                        case "BLPOP":
                            handleBlpop(outputStream, args);
                            break;
                        default:
                            outputStream.write("-ERR unknown command\r\n".getBytes());
                    }
                } else {
                    // This is a simple request, not an array.
                    // You can add logic here for commands without arguments, or error handling.
                    // For example, PING in a non-array format.
                    if (line.equalsIgnoreCase("PING")) {
                        outputStream.write("+PONG\r\n".getBytes());
                    } else {
                        outputStream.write("-ERR unknown command\r\n".getBytes());
                    }
                }
                outputStream.flush();
            }
        } catch (Exception e) {
            System.out.println("Error handling client: " + e);
            e.printStackTrace();
        }
    }

    private void handleEcho(OutputStream outputStream, List<String> args) throws IOException {
        String line = args.get(0);
        
            String resp = "+" + line + "\r\n";
            outputStream.write(resp.getBytes());
        
    }

    private void handleSet(OutputStream outputStream, List<String> args) throws IOException {
        if (args.size() == 2) {
            String key = args.get(0);
            String value = args.get(1);
            map.put(key, value);
            outputStream.write("+OK\r\n".getBytes());
        } else {
            String key = args.get(0);
            String value = args.get(1);
            map.put(key, value);
            String timeout = args.get(3);
            long duration = Long.parseLong(timeout);
            long now = System.currentTimeMillis();
            time.put(key, new Expiry(now, duration));
            outputStream.write("+OK\r\n".getBytes());
        }
    }

    private void handleGet(OutputStream outputStream, List<String> args) throws IOException {
        String key = args.get(0);
        String value = map.getOrDefault(key, "$-1\r\n");
        if (!value.equals("$-1\r\n")) {
            Expiry ex = time.get(key);
            if (ex != null) {
                long currentTime = System.currentTimeMillis();
                if (currentTime > ex.timestamp + ex.durationMs) {
                    value = "$-1\r\n";
                    map.remove(key);
                    time.remove(key);
                    outputStream.write(value.getBytes());
                } else {
                    outputStream.write(("$"+value.length()+"\r\n"+value+"\r\n").getBytes());
                }
            } else {
                outputStream.write(("$"+value.length()+"\r\n"+value+"\r\n").getBytes());
            }
        } else {
            outputStream.write(value.getBytes());
        }
    }
    private void handleLpush(OutputStream outputStream, List<String> args) throws IOException {
        if (args.size() < 2) {
            outputStream.write("-ERR wrong number of arguments for 'lpush' command\r\n".getBytes());
            outputStream.flush();
            return;
        }
        
        String firstKey = args.get(0);
        List<String> values = args.subList(1, args.size());
        int res = 0;

        lock.lock();
        try {
            Deque<String> queue = rmap.computeIfAbsent(firstKey, k -> new ArrayDeque<>());
            for (String value : values) {
                queue.addFirst(value);
            }
            res = queue.size();

            // Notify blocked clients if any
            // Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(firstKey);

            // This is the core fix for the "one-in-one-out" BLPOP behavior.
            // We only unblock one client and pass it the first pushed element.
            // We use a simple 'if' statement, not a 'while' loop.
            // if (blockedQueue != null && !blockedQueue.isEmpty() && !queue.isEmpty()) {
            //     CompletableFuture<String> future = blockedQueue.pollFirst();
            //     String element = queue.pollFirst();
                
            //     if (future != null && element != null) {
            //         System.out.println("@@@ LPUSH element " + element + " future " + System.identityHashCode(future));
            //         future.complete(element);
            //         System.out.println("@@@ LPUSH complete future " + System.identityHashCode(future));
            //     }
            // }

            // // Clean up the blocked queue if it's now empty
            // if (blockedQueue != null && blockedQueue.isEmpty()) {
            //     blockedClients.remove(firstKey);
            // }
        } finally {
            lock.unlock();
        }

        try {
            outputStream.write((":" + res + "\r\n").getBytes());
            outputStream.flush();
        } catch (IOException ignored) {}
    }

    private void handleLrange(OutputStream outputStream, List<String> args) throws IOException {
        String line;
        String key = args.get(0);
        String startStr = args.get(1);
        String endStr = args.get(2);
        
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
            if (start < 0) start = 0;
            if (end < 0) end = size + end;
            if (end < 0) end = 0;
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

    private void handleLlen(OutputStream outputStream, List<String> args) throws IOException {
        lock.lock();
        try {
            String key = args.get(0);
            Deque<String> queue = rmap.get(key);
            int len = (queue == null) ? 0 : queue.size();
            outputStream.write((":" + len + "\r\n").getBytes());
        } finally {
            lock.unlock();
        }
    }

    private void handleLpop(OutputStream outputStream, List<String> args) throws IOException {
        String key = args.get(0);
        int count = 1;
        if (args.size()>1) {
            count = Integer.parseInt(args.get(1));
        }
        lock.lock();
        try {
            Deque<String> queue = rmap.get(key);
            if (queue == null || queue.isEmpty()) {
                outputStream.write(("$-1\r\n").getBytes());
            } else {
                count = Math.min(count, queue.size());
                StringBuilder res = new StringBuilder();
                if (count == 1) {
                    String val = queue.pollFirst();
                    res.append("$"+val.length()+"\r\n"+val+"\r\n");
                } else {
                    res.append("*"+count+"\r\n");
                    for(int i = 0; i<count; i++) {
                        String val = queue.pollFirst();
                        res.append("$"+val.length()+"\r\n"+val+"\r\n");
                    }
                }
                outputStream.write(res.toString().getBytes());
            }
        } finally {
            lock.unlock();
        }
        
    }
    private void handleBlpop(OutputStream outputStream, List<String> args) throws IOException {
        // if (args.size() != 2) {
        //     outputStream.write("-ERR wrong number of arguments for 'blpop' command\r\n".getBytes());
        //     outputStream.flush();
        //     return;
        // }
         lock.lock();
        String key = args.get(0);
        double timeout;
        try {
            timeout = Double.parseDouble(args.get(1));
        } catch (NumberFormatException e) {
            outputStream.write("-ERR invalid timeout\r\n".getBytes());
            outputStream.flush();
            return;
        }
        
        // The rest of your core logic remains the same, but without any readLine() calls.
        // lock.lock();
        
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
                System.out.println("@@@BLPOP key "+key + " future "+System.identityHashCode(future)+" key exists "+blockedClients.containsKey(key));
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
                System.out.println("@@@BLPOP future wait "+System.identityHashCode(future));
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
                outputStream.write("$-1\r\n".getBytes());
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

    private void handleRpush(OutputStream outputStream, List<String> args) throws IOException {
        if (args.size() < 2) {
            outputStream.write("-ERR wrong number of arguments for 'rpush' command\r\n".getBytes());
            outputStream.flush();
            return;
        }
        
        String firstKey = args.get(0);
        List<String> values = args.subList(1, args.size());
        int res = 0;

        lock.lock();
        try {
            Deque<String> queue = rmap.computeIfAbsent(firstKey, k -> new ArrayDeque<>());
            for (String value : values) {
                queue.addLast(value);
            }
            res = queue.size();

            // Notify blocked clients if any
            Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(firstKey);
            if (blockedQueue == null) {
                System.out.println("@@@ rpush blocedQueue is null");
            }
            if (blockedQueue!=null && blockedQueue.isEmpty()) {
                System.out.println("@@@ bpush blockedQueue is empty "+firstKey);
            }
            if (queue.isEmpty()) {
                System.out.println("@@@ bpush queue is empty");
            }
            // This is the core fix for the "one-in-one-out" BLPOP behavior.
            // We only unblock one client and pass it the first pushed element.
            // We use a simple 'if' statement, not a 'while' loop.
            if (blockedQueue != null && !blockedQueue.isEmpty() && !queue.isEmpty()) {
                CompletableFuture<String> future = blockedQueue.pollFirst();
                String element = queue.pollFirst();
                
                if (future != null && element != null) {
                    System.out.println("@@@ RPUSH element " + element + " future " + System.identityHashCode(future));
                    future.complete(element);
                    System.out.println("@@@ RPUSH complete future " + System.identityHashCode(future));
                }
            }

            // Clean up the blocked queue if it's now empty
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
    // private void handleRpush(BufferedReader in, OutputStream outputStream, int argVar) throws IOException {
    //     String line;
    //     String key = null;
    //     List<String> values = new ArrayList<>();
    //     int totalSize;

    //     // Parse key and all values
    //     while (argVar > 0 && (line = in.readLine()) != null) {
    //         if (line.startsWith("*") || line.startsWith("$")) continue;

    //         if (key == null) {
    //             key = line;
    //         } else {
    //             values.add(line);
    //         }
    //         argVar--;
    //     }

    //     lock.lock();
    //     try {
    //         Deque<String> queue = rmap.computeIfAbsent(key, k -> new ArrayDeque<>());
    //         // Add values to the list
    //         for (String val : values) {
    //             queue.addLast(val);
    //         }
    //         // Total size is the remaining elements in the list
    //         totalSize = queue.size();
    //         // Serve blocked clients first
    //         Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(key);
    //         while (blockedQueue != null && !blockedQueue.isEmpty() && !queue.isEmpty()) {
    //             CompletableFuture<String> clientFuture = blockedQueue.pollFirst();
    //             String element = queue.pollFirst(); // give earliest element
    //             if (clientFuture != null && element != null) {
    //                 clientFuture.complete(element);
    //             }
    //         }

    //         // Clean up empty blocked queue
    //         if (blockedQueue != null && blockedQueue.isEmpty()) {
    //             blockedClients.remove(key);
    //         }

            
    //     } finally {
    //         lock.unlock();
    //     }

    //     // Respond to RPUSH client
    //     try {
    //         outputStream.write((":" + totalSize + "\r\n").getBytes());
    //         outputStream.flush();
    //     } catch (IOException ignored) {}
    // }
    // private void handleBlpop(BufferedReader in, OutputStream outputStream) throws IOException {
    //     String line;
    //     String key = null;
    //     double timeout = 0;
        
    //     // Parse key and timeout
    //     while ((line = in.readLine()) != null) {
    //         if (line.startsWith("*") || line.startsWith("$")) continue;

    //         if (key == null) {
    //             key = line;
    //         } else {
    //             try {
    //                 timeout = Double.parseDouble(line);
    //                 break;
    //             } catch (NumberFormatException e) {
    //                 outputStream.write("-ERR invalid timeout\r\n".getBytes());
    //                 outputStream.flush();
    //                 return;
    //             }
    //         }
    //     }

    //     final CompletableFuture<String> future = new CompletableFuture<>();

    //     boolean servedImmediately = false;

    //     lock.lock();
    //     try {
    //         Deque<String> queue = rmap.get(key);
    //         if (queue != null && !queue.isEmpty()) {
    //             // Serve immediately
    //             servedImmediately = true;
    //             String val = queue.pollFirst();
    //             outputStream.write(("*2\r\n$" + key.length() + "\r\n" + key +
    //                                 "\r\n$" + val.length() + "\r\n" + val + "\r\n").getBytes());
    //             outputStream.flush();
    //         } else {
    //             // No element, add to blocked clients
    //             blockedClients.computeIfAbsent(key, k -> new ArrayDeque<>()).add(future);
    //         }
    //     } finally {
    //         lock.unlock();
    //     }

    //     if (servedImmediately) return; // Already served, no need to wait

    //     // Handle future completion asynchronously
    //     final String blpopKey = key;
    //     future.thenAccept(value -> {
    //         try {
    //             outputStream.write(("*2\r\n$" + blpopKey.length() + "\r\n" + blpopKey +
    //                                 "\r\n$" + value.length() + "\r\n" + value + "\r\n").getBytes());
    //             outputStream.flush();
    //         } catch (IOException ignored) {}
    //     });

    //     // Handle timeout if specified
    //     if (timeout > 0) {
    //         CompletableFuture.delayedExecutor((long)(timeout * 1000), TimeUnit.MILLISECONDS)
    //                         .execute(() -> {
    //             if (!future.isDone()) {
    //                 future.completeExceptionally(new TimeoutException());
    //             }
    //         });
    //         final String blpopKey2 = key;
    //         future.exceptionally(ex -> {
    //             if (ex instanceof TimeoutException) {
    //                 lock.lock();
                    
    //                 try {
    //                     Deque<CompletableFuture<String>> blockedQueue = blockedClients.get(blpopKey2);
    //                     if (blockedQueue != null) {
    //                         blockedQueue.remove(future);
    //                         if (blockedQueue.isEmpty()) blockedClients.remove(blpopKey2);
    //                     }
    //                 } finally {
    //                     lock.unlock();
    //                 }

    //                 try {
    //                     outputStream.write("$-1\r\n".getBytes());
    //                     outputStream.flush();
    //                 } catch (IOException ignored) {}
    //             }
    //             return null;
    //         });
    //     }
    // }

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
