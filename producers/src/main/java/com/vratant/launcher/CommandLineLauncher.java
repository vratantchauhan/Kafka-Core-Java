package com.vratant.launcher;

import com.vratant.producer.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.vratant.producer.MessageProducer.propsMap;

public class CommandLineLauncher {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    public static String commandLineStartLogo(){

        return " __      _____________.____   _________  ________      _____  ___________                       \n" +
                "/  \\    /  \\_   _____/|    |  \\_   ___ \\ \\_____  \\    /     \\ \\_   _____/                       \n" +
                "\\   \\/\\/   /|    __)_ |    |  /    \\  \\/  /   |   \\  /  \\ /  \\ |    __)_                        \n" +
                " \\        / |        \\|    |__\\     \\____/    |    \\/    Y    \\|        \\                       \n" +
                "  \\__/\\  / /_______  /|_______ \\______  /\\_______  /\\____|__  /_______  /                       \n" +
                "       \\/          \\/         \\/      \\/         \\/         \\/        \\/                        \n" +
                "  __                                                                                            \n" +
                "_/  |_  ____                                                                                    \n" +
                "\\   __\\/  _ \\                                                                                   \n" +
                " |  | (  <_> )                                                                                  \n" +
                " |__|  \\____/                                                                                   \n" +
                "                                                                                                \n" +
                "_________                                           .___.____    .__                            \n" +
                "\\_   ___ \\  ____   _____   _____ _____    ____    __| _/|    |   |__| ____   ____               \n" +
                "/    \\  \\/ /  _ \\ /     \\ /     \\\\__  \\  /    \\  / __ | |    |   |  |/    \\_/ __ \\              \n" +
                "\\     \\___(  <_> )  Y Y  \\  Y Y  \\/ __ \\|   |  \\/ /_/ | |    |___|  |   |  \\  ___/              \n" +
                " \\______  /\\____/|__|_|  /__|_|  (____  /___|  /\\____ | |_______ \\__|___|  /\\___  >             \n" +
                "        \\/             \\/      \\/     \\/     \\/      \\/         \\/       \\/     \\/              \n" +
                " ____  __.       _____ __             __________                   .___                         \n" +
                "|    |/ _|____ _/ ____\\  | _______    \\______   \\_______  ____   __| _/_ __   ____  ___________ \n" +
                "|      < \\__  \\\\   __\\|  |/ /\\__  \\    |     ___/\\_  __ \\/  _ \\ / __ |  |  \\_/ ___\\/ __ \\_  __ \\\n" +
                "|    |  \\ / __ \\|  |  |    <  / __ \\_  |    |     |  | \\(  <_> ) /_/ |  |  /\\  \\__\\  ___/|  | \\/\n" +
                "|____|__ (____  /__|  |__|_ \\(____  /  |____|     |__|   \\____/\\____ |____/  \\___  >___  >__|   \n" +
                "        \\/    \\/           \\/     \\/                                \\/           \\/    \\/       ";
    }

    public static String BYE(){

        return "_______________.___.___________\n" +
                "\\______   \\__  |   |\\_   _____/\n" +
                " |    |  _//   |   | |    __)_ \n" +
                " |    |   \\\\____   | |        \\\n" +
                " |______  // ______|/_______  /\n" +
                "        \\/ \\/               \\/ ";
    }

    public static void launchCommandLine() throws ExecutionException, InterruptedException {
        boolean cliUp = true;
        while (cliUp){
            Scanner scanner = new Scanner(System.in);
            userOptions();  //just to print options on the command line.
            String option = scanner.next();
            logger.info("Selected Option is : {} ", option);
            switch (option) {
                case "1":
                    acceptMessageFromUser(option);
                    break;
                case "2":
                    cliUp = false;
                    break;
                default:
                    break;

            }
        }
    }

    public static void userOptions(){
        List<String> userInputList = new ArrayList<>();
        userInputList.add("1: Kafka Producer");
        userInputList.add("2: Exit");
        System.out.println("Please select one of the below options:");
        for(String userInput: userInputList ){
            System.out.println(userInput);
        }
    }
    public static MessageProducer init(){

        Map<String, Object> producerProps = propsMap();
        MessageProducer messageProducer = new MessageProducer(producerProps);
        return messageProducer;
    }

    public static void publishMessage(MessageProducer messageProducer, String input) throws ExecutionException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(input, "-");
        Integer noOfTokens = stringTokenizer.countTokens();
        switch (noOfTokens){
            case 1:
                messageProducer.publishMessageSync(null,stringTokenizer.nextToken());
                break;
            case 2:
                messageProducer.publishMessageSync(stringTokenizer.nextToken(),stringTokenizer.nextToken());
                break;
            default:
                break;
        }
    }

    public static void acceptMessageFromUser(String option) throws ExecutionException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        boolean flag= true;
        while (flag){
            System.out.println("Please Enter a Message to produce to Kafka:");
            String input = scanner.nextLine();
            logger.info("Entered message is {}", input);
            if(input.equals("00")) {
                flag = false;
            }else {
                MessageProducer messageProducer = init();
                publishMessage(messageProducer, input);
                messageProducer.close();
            }
        }
        logger.info("Exiting from Option : " + option);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.out.println(commandLineStartLogo());
        launchCommandLine();
        System.out.println(BYE());


    }
}