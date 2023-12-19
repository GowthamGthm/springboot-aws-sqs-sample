package com.javatodev.app.service;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@NoArgsConstructor
public class MessageHandler implements AsyncHandler {

    @Override
    public void onError(Exception e) {
        e.printStackTrace();
    }

    @Override
    public void onSuccess(AmazonWebServiceRequest request, Object o) {
//        log.info(o.toString());
    }
}