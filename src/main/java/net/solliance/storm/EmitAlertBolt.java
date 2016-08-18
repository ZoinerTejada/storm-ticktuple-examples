package net.solliance.storm;

import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


public class EmitAlertBolt extends BaseBasicBolt{

    private static final long serialVersionUID = 1L;

    protected double minAlertTemp;
    protected double maxAlertTemp;

    protected int alertCounter;

    public EmitAlertBolt(double minTemp, double maxTemp) {
        minAlertTemp = minTemp;
        maxAlertTemp = maxTemp;

        alertCounter = 0;
    }

public void execute(Tuple input, BasicOutputCollector collector) {

    if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) //Handle Tick Tuple
            && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
        System.out.println("========= EmitAlert: " + alertCounter + " alerts emitted in tick window. ++++++++++++++++");
        alertCounter = 0;
    }
    else { //Handle data tuple
        double tempReading = input.getDouble(0);
        String createDate = input.getString(1);
        String deviceId = input.getString(2);

        if (tempReading > maxAlertTemp) {

            collector.emit(new Values(
                    "reading above bounds",
                    tempReading,
                    createDate,
                    deviceId
            ));
            System.out.println("Emitting above bounds: " + tempReading);

            alertCounter++;
        } else if (tempReading < minAlertTemp) {
            collector.emit(new Values(
                    "reading below bounds",
                    tempReading,
                    createDate,
                    deviceId
            ));
            System.out.println("Emitting below bounds: " + tempReading);

            alertCounter++;
        }
    }
}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("reason","temp","createDate", "deviceId"));
    }
}