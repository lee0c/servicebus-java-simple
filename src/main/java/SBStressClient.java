import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.TransportType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SBStressClient {

    static final String CONNECTION_STRING_ENV_VAR = "SERVICEBUS_CONNECTION_STRING";
    static final String PROXY_HOSTNAME_ENV_VAR = "PROXY_HOSTNAME";
    static final String PROXY_PORT_ENV_VAR = "PROXY_PORT";

    static String logFileName;
    static BufferedWriter writer;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("FORMAT: <jar> [-r | -s]");
            System.exit(1);
        } else {
            if (args[0].equals("-s")) {
                logFileName = "./sender.log";
            } else if (args[0].equals("-r")) {
                logFileName = "./receiver.log";
            } else {
                System.err.println("FORMAT: <jar> [-r | -s]");
                System.exit(1);
            }
        }

        setUpProxy();
        ConnectionStringBuilder connStrBuilder = getConnStringBuilderForProxy();

        try {
            QueueClient client = new QueueClient(connStrBuilder, ReceiveMode.PEEKLOCK);
            writer = new BufferedWriter(new FileWriter(logFileName));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.out.println("Closing FileWriter");
                    try {
                        writer.close();
                    } catch (IOException e) {
                        System.err.printf("Exception: %s", e.getMessage());
                        e.printStackTrace(System.err);
                    }
                }
            });

            if (args[0].equals("-s")) {
                sendMessages(client);
            } else if (args[0].equals("-r")) {
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                registerReceiver(client, executorService);
            }
        } catch (Exception e) {
            System.err.printf("Exception: %s", e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    static void sendMessages(QueueClient sendClient) throws Exception {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            Message message = new Message();
            message.setMessageId(Integer.toString(i));
            sendClient.sendAsync(message).thenRunAsync( () ->  {
                try{
                    writer.write(String.format("Message acknowledged: Id = %s; DateTime = %s; Instant = %d\n",
                            message.getMessageId(), Instant.now().toString(), Instant.now().getEpochSecond()));
                } catch (IOException e) {
                    System.err.printf("Exception: %s", e.getMessage());
                    e.printStackTrace(System.err);
                }
            });

            // 30s wait
            Thread.sleep(30000);
        }
    }

    static void registerReceiver(QueueClient receiveClient, ExecutorService executorService) throws Exception {
        receiveClient.registerMessageHandler(new IMessageHandler() {
            public CompletableFuture<Void> onMessageAsync(IMessage message) {
                try {
                    writer.write(String.format("Message received: Id = %s; DateTime = %s; Instant = %d\n",
                            message.getMessageId(), Instant.now().toString(), Instant.now().getEpochSecond()));
                } catch (IOException e) {
                    System.err.printf("Exception: %s", e.getMessage());
                    e.printStackTrace(System.err);
                }
                return CompletableFuture.completedFuture(null);
            }

            public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                System.out.printf(exceptionPhase + "-" + throwable.getMessage());
            }
        }, new MessageHandlerOptions(), executorService);
    }

    static ConnectionStringBuilder getConnStringBuilderForProxy() {
        ConnectionStringBuilder connStrBuilder = new ConnectionStringBuilder(System.getenv(CONNECTION_STRING_ENV_VAR), "basicqueue");
        connStrBuilder.setTransportType(TransportType.AMQP_WEB_SOCKETS);

        return connStrBuilder;
    }

    static void setUpProxy() {
        final String proxyHostname = System.getenv(PROXY_HOSTNAME_ENV_VAR);
        final int proxyPort = Integer.parseInt(System.getenv(PROXY_PORT_ENV_VAR));

        System.out.printf("Setting up ProxySelector with proxy address %s:%d\n", proxyHostname, proxyPort);

        final ProxySelector systemDefaultSelector = ProxySelector.getDefault();
        ProxySelector.setDefault(new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                if (uri != null && uri.getHost() != null) {
                    List<Proxy> proxies = new LinkedList<Proxy>();
                    proxies.add(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHostname, proxyPort)));
                    return proxies;
                }
                return systemDefaultSelector.select(uri);
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                if (uri == null || sa == null || ioe == null) {
                    throw new IllegalArgumentException("Arguments can't be null.");
                }
                systemDefaultSelector.connectFailed(uri, sa, ioe);
            }
        });
    }
}

