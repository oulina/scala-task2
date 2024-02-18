package org.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface Handler {
    default Duration timeout() {
        return Duration.ofSeconds(2);
    }

    default void performOperation() {
        Client client = new Client() {
            @Override
            public Event readData() {
                ArrayList<Address> addresses = new ArrayList<>();
                addresses.add(new Address("1", "1"));
                addresses.add(new Address("2", "2"));
                addresses.add(new Address("3", "3"));
                return new Event(addresses, new Payload("origin", new byte[]{1, 2, 4}));
            }

            @Override
            public Result sendData(Address dest, Payload payload) {
                if (dest.datacenter().equals("1")) {
                    //return Result.REJECTED;
                }
                return Result.ACCEPTED;
            }
        };
        Event event = client.readData();

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        if (event != null) {
            try {
                List<Address> addresses = event.recipients();
                List<Thread> threads = new ArrayList<>();
                int countAddress = 100;
                for(int i = 0; i < addresses.size(); i += countAddress) {
                    List<Address> chunk = event.recipients().subList(i, Math.min(i + countAddress,
                            event.recipients().size()));
                    Thread thread = new Thread(new TaskThreadChunkAddress(chunk, executor, client, event.payload()));
                    thread.start();
                    threads.add(thread);
                }
                for(Thread thread: threads) {
                    thread.join();
                }

            } catch (Exception err) {
                err.printStackTrace();
            }

        }
        executor.shutdown();
    }

    class TaskThreadChunkAddress implements Runnable {
        List<Address> chunk;
        ExecutorService executor;
        Client client;
        Payload payload;

        public TaskThreadChunkAddress(List<Address> chunk, ExecutorService executor, Client client, Payload payload) {
            this.chunk = chunk;
            this.executor = executor;
            this.client = client;
            this.payload = payload;
        }

        @Override
        public void run() {
            for (Address address : chunk) {
                executor.execute(new TaskSender(client, address, payload));
            }
        }
    }

    class TaskSender implements Runnable {
        Address address;
        Payload payload;
        Client client;

        public TaskSender(Client client, Address address, Payload payload) {
            this.address = address;
            this.payload = payload;
            this.client = client;
        }

        public void run() {
            try {
                System.out.println("Run: " + Thread.currentThread().getName());
                System.out.println(address);
                Result result;
                do {
                    result = client.sendData(address, payload);
                    if (result == Result.REJECTED) {
                        Thread.sleep(new Handler() {
                        }.timeout().toMillis());
                    }
                } while (result != Result.ACCEPTED);
                System.out.println("Success: " + address);
            } catch (Exception err) {
                err.printStackTrace();
            }
        }
    }
}
