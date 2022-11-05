package com.upmasters.kafkaproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upmasters.kafkaproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@Component
@Slf4j
public class LibraryEventProducer {

  @Autowired
  KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  ObjectMapper objectMapper;

  public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);

    ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onFailure(final Throwable ex) {
        handleFailure(key, value, ex);
      }

      @Override
      public void onSuccess(final SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }
    });

  }

  private void handleFailure(final Integer key, final String value, final Throwable ex) {
    log.error("Error sending the message and the exception is {}", ex.getMessage());
    try{
      throw ex;
    } catch (Throwable throwable){
      log.error("Error in OnFailure: {}", throwable.getMessage());
    }
  }

  private void handleSuccess(final Integer key, final String value, final SendResult<Integer, String> result) {

    log.info("Message Sent SuccessFully for the key: {} and the value is {} , partion is {}", key, value, result.getRecordMetadata().partition());

  }

}