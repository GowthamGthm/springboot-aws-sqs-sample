package com.javatodev.app.service;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageQueueService {

    @Value("${app.config.message.queue.topic}")
    private String messageQueueTopic;

    private final AmazonSQS amazonSQSClient;

    private final AmazonSQSAsync amazonSQSAsyncClient;

//    @Scheduled(fixedDelay = 5000) //executes on every 5 second gap.
    public void receiveMessages() {
        try {
            String queueUrl = amazonSQSClient.getQueueUrl(messageQueueTopic).getQueueUrl();
            log.info("Reading SQS Queue done: URL {}", queueUrl);

            ReceiveMessageResult receiveMessageResult = amazonSQSClient.receiveMessage(queueUrl);

            if (!receiveMessageResult.getMessages().isEmpty()) {
                Message message = receiveMessageResult.getMessages().get(0);
                log.info("------------------------------------------------");
                log.info("Incoming Message From SQS {}", message.getMessageId());
                log.info("Message Body {}", message.getBody());
                log.info("------------------------------------------------");
                processInvoice(message.getBody());
                amazonSQSClient.deleteMessage(queueUrl, message.getReceiptHandle());
            }

        } catch (QueueDoesNotExistException e) {
            log.error("Queue does not exist {}", e.getMessage());
        }
    }


    @Scheduled(fixedDelay = 5000)
    public void getMessagesInBulk() {
        AtomicInteger atomNumber = new AtomicInteger(0);
        log.info("------------- scheduler running ---------");
        String qUrl = amazonSQSAsyncClient.getQueueUrl(messageQueueTopic).getQueueUrl();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(qUrl);
        receiveMessageRequest.setMaxNumberOfMessages(10);
        receiveMessageRequest.setWaitTimeSeconds(20);

        Future<ReceiveMessageResult> asyncResult = amazonSQSAsyncClient.receiveMessageAsync(receiveMessageRequest , new MessageHandler());
        try {
            ReceiveMessageResult result = asyncResult.get();
            for (Message message : result.getMessages()) {
                log.info("======================= " + atomNumber.incrementAndGet() + " =====================");
                log.info(message.getBody());
                log.info(message.getMessageId());
                amazonSQSAsyncClient.deleteMessageAsync(new DeleteMessageRequest(qUrl , message.getReceiptHandle()));
            }

        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getLocalizedMessage());
        }
    }

    private void processInvoice(String body) {
        log.info("Processing invoice");
    }

}