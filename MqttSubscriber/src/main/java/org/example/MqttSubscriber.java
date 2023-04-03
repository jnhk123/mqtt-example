package org.example;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class MqttSubscriber implements MqttCallback {
    private MqttClient client;

    public MqttSubscriber() {
        try {
            String brokerUrl = "tcp://localhost:1883";
            String clientId = "mqtt-subscriber-ftp";
            MemoryPersistence persistence = new MemoryPersistence();
            client = new MqttClient(brokerUrl, clientId, persistence);
            client.setCallback(this);
            client.connect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void subscribe(String topic) {
        try {
            client.subscribe(topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost: " + throwable.getMessage());
        System.out.println("[" + getDate() + "] Service End");
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
        System.out.println("[" + getDate() + "] Message arrived: " + new String(mqttMessage.getPayload()));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        System.out.println("Delivery complete");
    }

    public static void main(String[] args) {
        System.out.println("[" + getDate() + "] Service Start");
        MqttSubscriber subscriber = new MqttSubscriber();
        subscriber.subscribe("iderms/event/ftp-v2");
    }

    public static String getDate() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formatDate = sdf.format(cal.getTime());
        return formatDate;
    }
}