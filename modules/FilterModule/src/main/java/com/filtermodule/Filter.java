package com.filtermodule;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageProperty;

public class Filter {
    private int temperatureThreshold;

    public Filter(int temperatureThreshold){
        this.temperatureThreshold = temperatureThreshold;
    }
    public Message filterMessage(Message message){
        String messageStr = new String(message.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET);
        JsonElement element = new JsonParser().parse(messageStr);
        JsonObject object = element.getAsJsonObject();
        int temperature = object.getAsJsonObject("machine").getAsJsonPrimitive("temperature").getAsInt();

        if(temperature > temperatureThreshold) {
            Message filteredMessage = new Message(messageStr);
            MessageProperty[] props = message.getProperties();
            for (MessageProperty prop : props) {
                filteredMessage.setProperty(prop.getName(), prop.getValue());
            }
            filteredMessage.setProperty("MessageType", "Alert");
            return filteredMessage;
        }
        else {
            return null;
        }
    }

    public void setThreshold(int value) {
        this.temperatureThreshold = value;
    }
}